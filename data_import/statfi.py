import os
import re
import pandas as pd
from pandas_pcaxis import PxParser
from utils.quilt import update_node_from_pcaxis


def rename_column_that_contains(df, s, to):
    matches = list(filter(lambda x: s in x, df.columns))
    if len(matches) != 1:
        raise Exception('Invalid column matches for "%s": %s' % (s, matches))
    return df.rename(columns={matches[0]: to}, inplace=True)


def get_fuel_classification(include_units=False):
    URL = 'https://www.stat.fi/static/media/uploads/tup/khkinv/khkaasut_polttoaineluokitus_2019_v2.xlsx'
    URL = 'data/khkaasut_polttoaineluokitus_2019_v2.xlsx'

    sheets = pd.read_excel(URL, header=1, sheet_name=None)

    df = list(sheets.values())[0]
    assert df.columns[0] == 'Koodi'
    assert df.columns[1] == 'Nimike'
    assert 'Unnamed' in df.columns[2]

    col_map = {
        'Koodi': 'code',
        'Nimike': 'name',
        df.columns[2]: 'name2',
    }

    rename_column_that_contains(df, 'CO2', 'co2e_emission_factor')
    rename_column_that_contains(df, 'Huom', 'note')
    rename_column_that_contains(df, 'määrä-yksikkö', 'quantity_unit')
    rename_column_that_contains(df, 'oletus-lämpöarvo', 'calorific_value')

    output_cols = ['code', 'name', 'co2e_emission_factor', 'quantity_unit', 'calorific_value']

    df = df.rename(columns=col_map)
    df = df[df.code.notnull()]
    df.code = df.code.astype(int).astype(str)
    for cat_level in (1, 2, 3):
        cats = df[df.code.str.len() == cat_level].set_index('code')
        cat_names = cats.name
        if cat_names.isna().all():
            cat_names = cats.name2

        col_name = 'category%d' % cat_level
        output_cols.append(col_name)
        df[col_name] = df.code.str.slice(0, cat_level).map(cat_names)

    df.co2e_emission_factor = pd.to_numeric(df.co2e_emission_factor, errors='coerce')
    df = df[df.co2e_emission_factor.notnull()]
    df = df.drop(columns='name').rename(columns=dict(name2='name'))
    df['is_bio'] = df.note.str.strip() == 'BIO'
    output_cols.append('is_bio')

    df.calorific_value = pd.to_numeric(df.calorific_value, errors='coerce')
    df = df[output_cols].set_index('code')

    # Replace 1000 m3 with 1000 m^3 which pint recognizes
    QUANTITY_MAP = {
        "1000 m3": "1000 m^3",
    }
    df.quantity_unit = df.quantity_unit.map(lambda x: QUANTITY_MAP.get(x, x))

    if include_units:
        df.co2e_emission_factor = df.co2e_emission_factor.astype('pint[t/TJ]')
        # calorific value "count" is specified by column quantity_unit
        df.calorific_value = df.calorific_value.astype('pint[GJ/count]')

    out = df

    df = list(sheets.values())[2]
    assert df.columns[0] == 'Code'
    df = df.rename(columns={df.columns[2]: 'name_en', 'Code': 'code'})
    df = df[['name_en', 'code']].dropna()
    df.code = df.code.astype(int).astype(str)
    out = out.merge(df, on='code')

    return out


def get_energy_production_stats():
    last_heading = None

    def clean_method(x):
        global last_heading

        x = re.sub(r'[0-9]\)', '', x).replace('Combined heat and power', 'CHP')\
            .replace('CHP/ ', 'CHP/').replace('CHP total', 'CHP').strip()
        x = re.sub(r' power$', '', x)
        x = re.sub(r' energy$', '', x)
        if x in ('Electricity', 'District heat', 'Industrial steam'):
            return (last_heading, x)
        else:
            if 'electricity' in x.lower() or 'separate electricity' in last_heading.lower() or \
                    any([t == x.lower() for t in ('wind', 'solar', 'nuclear', 'conventional condensing')]):
                energy_type = 'Electricity'
            elif 'separate heat' in last_heading.lower():
                energy_type = 'District heat'
            else:
                energy_type = None
            last_heading = x
            return (x, energy_type)

    COLUMNS = [
        'Method_sv', 'Method', 'Method_fi',
        'Hydro', 'Wind', 'Solar', 'Nuclear',
        'Hard coal', 'Oil', 'Natural gas', 'Peat',
        'Wood fuels', 'Other renewables', 'Other fossil fuels', 'Other energy sources',
        'Net imports of electricity', 'Total', 'Net supply',
        'Production of district heat', 'Production of industrial steam', 'Production efficiency',
    ]
    sheets = pd.read_excel(
        'data/t03_04_4.xls', header=15, names=COLUMNS, usecols='A:U', na_values=['–'],
        sheet_name=None
    )
    all_dfs = []
    for name, df in sheets.items():
        year = re.search(r'\((\d{4})\)', name).groups()[0]
        df = df.dropna(axis=0, subset=['Method']).copy()
        df['Method'] = df.Method.apply(clean_method)
        df['Energy type'] = df.Method.apply(lambda x: x[1])
        df['Method'] = df.Method.apply(lambda x: x[0])
        df['Year'] = int(year)
        df.drop(columns=['Method_sv', 'Method_fi', 'Total', 'Production efficiency'], inplace=True)
        all_dfs.append(df)

    df = pd.concat(all_dfs, ignore_index=True)
    df = df.melt(id_vars=['Method', 'Year', 'Energy type'], var_name='Energy source', value_name='Energy').dropna()
    df.Energy = df.Energy.astype(float)
    df = df[~df.Method.isin([
        'Energy sources of separate electricity generation total', 'CHP', 'Separate heat production', 'Total',
    ])]
    df.Method = df.Method.map(lambda x: {
        'Electricity generation / net imports': 'Electricity generation',
    }.get(x, x))
    df['Unit'] = 'TJ'
    gwh_rows = df['Method'].isin([
        'Electricity generation', 'Production of district heat', 'Production of industrial heat/steam'
    ]) | df['Energy source'].isin([
        'Net supply', 'Production of district heat', 'Production of industrial steam'
    ])
    df.loc[gwh_rows, 'Unit'] = 'GWh'
    df.loc[df.Method.str.contains('CO2'), 'Unit'] = 'megatonne'

    FUEL_MAP = {
        'Hard coal': '1212',
        'Oil': '1134',  # Gasoil, low sulphur (heating fuel oil)
        'Natural gas': '1311',
        'Peat': '211',  # Milled peat
        'Wood fuels': '3129',  # Other industrial wood residue
        'Other renewables': '3179',  # Muut kasviperäiset polttoaineet
        'Other fossil fuels': '126',  # CO-gas??
    }
    df['Fuel code'] = df['Energy source'].map(FUEL_MAP).astype(str)

    return df


def update_quilt_datasets():
    QUILT_TARGET = 'jyrjola/statfi'
    from quilt.data.jyrjola import statfi as node
    import quilt
    import requests_cache
    requests_cache.install_cache()

    df = get_fuel_classification()
    df.to_csv('fuel_classification.csv')
    print(df)
    exit()
    node._set(['fuel_classification'], df)
    quilt.build(QUILT_TARGET, node)
    quilt.push(QUILT_TARGET, is_public=True)


def get_pop():
    parser = PxParser()
    file = parser.parse(open('data/tilastokeskus/statfin_vaerak_pxt_11re.px', 'r', encoding='windows-1252').read())
    node = update_node_from_pcaxis('jyrjola/tilastokeskus', 'vaestorakenne', file)
    print(node)


def walk_files():
    i = 0
    parser = PxParser()
    file = parser.parse(open('data/tilastokeskus/statfin_vaerak_pxt_11re.px', 'r', encoding='windows-1252').read())

    skip = True

    for dirname, subdirs, files in os.walk('data/tilastokeskus'):
        for fn in files:
            if not fn.endswith('.px'):
                continue

            if fn == 'statfin_kans_pxt_11l5.px':
                skip = False
            if skip:
                continue

            full_fn = os.path.join(dirname, fn)
            print(full_fn)
            f = open(full_fn, 'r', encoding='windows-1252')
            pxf = parser.parse(f.read())
            df = pxf.to_df(melt=True)
            print(df)

        i += 1
        if i == 10:
            break


def import_all():
    import requests_cache
    import requests
    import io
    import time

    requests_cache.install_cache('statfi')

    resp = requests.get('http://pxnet2.stat.fi/database/StatFin/StatFin_rap.csv')
    resp.raise_for_status()
    df = pd.read_csv(io.StringIO(resp.content.decode('iso-8859-15')), header=0, delimiter=';', dtype=str)
    #print(df[['pathname', 'fileupdate', 'NEXT-UPDATE']])

    for p in df['pathname']:
        print(p)
        out_name = p.split('/StatFin/')[1]
        dirname = os.path.dirname(out_name)
        fname = os.path.basename(out_name)
        dirname = 'data/tilastokeskus/' + dirname
        if not os.path.exists(dirname):
            os.makedirs(dirname)
        full_fn = os.path.join(dirname, fname)
        if os.path.exists(full_fn):
            continue
        resp = requests.get(p)
        resp.raise_for_status()
        f = open(full_fn, 'wb')
        f.write(resp.content)
        time.sleep(0.2)


if __name__ == '__main__':
    # import_all()
    walk_files()
