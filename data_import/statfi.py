import pandas as pd
import pintpandas  # noqa


def rename_column_that_contains(df, s, to):
    matches = list(filter(lambda x: s in x, df.columns))
    if len(matches) != 1:
        raise Exception('Invalid column matches for "%s": %s' % (s, matches))
    return df.rename(columns={matches[0]: to}, inplace=True)


def get_fuel_classification(include_units=False):
    URL = 'https://www.stat.fi/static/media/uploads/tup/khkinv/khkaasut_polttoaineluokitus_2019_v2.xlsx'
    df = pd.read_excel(URL, header=1)
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

    df.rename(columns=col_map, inplace=True)
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

    return df
