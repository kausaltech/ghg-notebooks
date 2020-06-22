import glob
import re
from datetime import timedelta
import pytz
import pandas as pd
import glob


LOCAL_TZ = pytz.timezone('Europe/Helsinki')


def _determine_fuel_mapping(df):
    import Levenshtein
    from . import statfi

    HARDCODED_MAPS = {
        'Keskiraskaat öljyt (kevyt polttoöljy)': 'Muut keskiraskaat öljyt',
        'Raskaat öljyt': 'Raskas polttoöljy, rikkipitoisuus',
        'Muu hiili': 'Muu erittelemätön hiili',
        'Kasviperäiset polttoaineet': 'Muut kasviperäiset polttoaineet',
        'Eläinperäiset polttoaineet': 'Muut eläinperäiset polttoaineet',
        'Biokaasu': 'Muut biokaasut',
        'Ongelmajätteet': 'Vaarallinen jäte',
        'Muut sivu- ja jätetuotteet': 'Muut jätteet',
    }

    fuels = statfi.get_fuel_classification()

    print('ENERGY_SOURCE_MAP = {')
    for orig_col in df.columns:
        def get_similarity(x):
            return Levenshtein.jaro_winkler(col, x)

        if orig_col in HARDCODED_MAPS:
            col = HARDCODED_MAPS[orig_col]
        else:
            col = orig_col

        fuels['similarity'] = fuels.name.map(get_similarity)
        matches = fuels.sort_values('similarity', ascending=False).head(4)
        for fuel_id, row in matches.iterrows():
            if row.similarity > 0.9:
                if orig_col == row['name']:
                    comment = ''
                else:
                    comment = '  # %s' % row['name']
                print('    "%s": "%s",%s' % (orig_col, fuel_id, comment))
            else:
                print('    # no match for %s' % orig_col)
            break
    print('}')


ENERGY_SOURCE_MAP = {
    "Keskiraskaat öljyt (kevyt polttoöljy)": "1139",  # Muut keskiraskaat öljyt
    "Kevyt polttoöljy": "1134",  # Kevyt polttoöljy, vähärikkinen
    "Raskaat öljyt": "1145",  # Raskas polttoöljy, rikkipitoisuus <0,5%
    "Raskas polttoöljy": "1145",
    "Kierrätys- ja jäteöljyt": "116",
    "Muut öljytuotteet": "119",
    "Kivihiili ja antrasiitti": "1212",  # Kivihiili
    "Kivihiili": "1212",
    "Muu hiili": "1229",  # Muu erittelemätön hiili
    "Koksi": "123",
    "Maakaasu": "1311",
    "Nesteytetty maakaasu (LNG)": "1312",
    "Jyrsinturve": "211",
    "Palaturve": "212",
    "Turvepelletit ja -briketit": "213",
    "Halot, rangat ja pilkkeet": "3111",
    "Kokopuu- tai rankahake": "3112",
    "Metsätähdehake tai -murske": "3113",
    "Kantomurske": "3114",  # Kantomurske (aik. kantohake)
    "Energiapaju (ja muu lyhytkiertoviljelty puu)": "3115",
    "Kuori": "3121",
    "Sahanpuru": "3122",
    "Puutähdehake tai -murske": "3123",
    "Kutterilastut, hiontapöly yms.": "3124",
    "Erittelemätön teollisuuden puutähde": "3128",
    "Muu teollisuuden puutähde": "3129",
    "Puunjalostusteollisuuden jäteliemet": "313",
    "Puunjalostusteollisuuden sivu- ja jätetuotteet": "313",  # Puunjalostusteollisuuden jäteliemet
    "Kierrätyspuu": "315",
    "Puupelletit ja -briketit": "316",
    "Kasviperäiset polttoaineet": "3179",  # Muut kasviperäiset polttoaineet
    "Eläinperäiset polttoaineet": "3189",  # Muut eläinperäiset polttoaineet
    "Biokaasu": "3219",  # Muut biokaasut
    "Biopolttonesteet": "3221",  # Biopolttoöljy
    "Kierrätyspolttoaineet": "3231",
    "Purkupuu": "3232",
    "Kyllästetty puu": "3233",
    "Siistausliete": "3234",
    "Jätepelletit": "3235",
    "Kumijätteet": "3236",
    "Yhdyskuntajäte/sekajäte": "3238",  # Yhdyskuntajäte / sekajäte
    "Muut sekapolttoaineet": "3239",
    "Muovijätteet": "4911",
    "Ongelmajätteet": "4913",  # Vaarallinen jäte (ent. ongelmajäte)
    "Muut sivu- ja jätetuotteet": "4919",  # Muut jätteet
    "Vety": "498",
}


def _process_distring_heating_excel(df):
    df = df.drop(columns=df.columns[0])
    col_map = {
        df.columns[0]: 'Operator',
        df.columns[1]: 'OperatorName',
    }
    df = df.rename(columns=col_map)
    # Drop rows that don't have a producer id
    df = df[~(df.Operator.isnull() | df.OperatorName.isnull())]

    df.OperatorName = df.OperatorName.astype(str)

    # Make sure data columns have numbers in them
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    return df


def _process_district_heating_fuel_stats(fname):
    df = pd.read_excel(fname, sheet_name='Taul3', header=2, converters={1: lambda x: str(x)})
    df = _process_distring_heating_excel(df)

    # Convert columns into rows
    df = df.melt(id_vars=['Operator', 'OperatorName'], var_name='Quantity', value_name='Value')
    # Drop NaN rows
    df = df[~df.Value.isna()]
    df = df[~(df.Quantity == 'Polttoöljy yhteensä')]

    df['StatfiFuelCode'] = df.Quantity.map(ENERGY_SOURCE_MAP)
    df['Unit'] = 'GWh'

    return df


def _process_distring_heating_production_stats(fname):
    df = pd.read_excel(fname, sheet_name='Taul1', header=7)
    df = _process_distring_heating_excel(df)
    COLS_TO_KEEP = [
        'Nettotuotanto polttoaineilla', 'Lämmön talteenotto tai lämpöpumpun tuotanto', 'Osto', 'Kulutus',
        'Yhteensä', 'Käyttö', 'Toimitus', 'Verkkohäviöt ja mittauserot', 'Kaukolämmön nettotuotannosta yhteistuotantona',
        'Kaukolämmön tuotantoon liittyvä sähkön nettotuotanto', 'Kaukolämmön tuotantoon kulunut sähkö',
        'Kaukolämmön siirron pumppuenergia'
    ]
    COL_UNITS = 'GWh GWh GWh GWh GWh GWh GWh GWh GWh GWh MWh MWh'.split()

    cols_to_drop = [col for col in df.columns if col not in COLS_TO_KEEP + ['Operator', 'OperatorName']]
    df = df.drop(columns=cols_to_drop).rename(columns=dict(Kulutus='Käyttö'))

    # Convert columns into rows
    df = df.melt(id_vars=['Operator', 'OperatorName'], var_name='Quantity', value_name='Value')
    # Drop NaN rows
    df = df[~df.Value.isna()]

    unit_map = {x[0]: x[1] for x in zip(COLS_TO_KEEP, COL_UNITS)}
    df['Unit'] = df.Quantity.map(unit_map)
    return df


def get_district_heating_fuel_stats():
    dfs = []
    for fname in glob.glob('data/energiateollisuus/Vuositaulukot*'):
        year = int('20' + re.search(r'(\d\d)', fname).groups()[0])
        fuel_df = _process_district_heating_fuel_stats(fname)
        fuel_df['Year'] = year
        dfs.append(fuel_df)

    df = dfs[0]
    all_years = df.append(dfs[1:]).set_index('Year', 'Operator').sort_index()
    return all_years.reset_index()


def get_district_heating_production_stats():
    dfs = []
    for fname in glob.glob('data/energiateollisuus/Vuositaulukot*'):
        year = int('20' + re.search(r'(\d\d)', fname).groups()[0])
        fuel_df = _process_distring_heating_production_stats(fname)
        fuel_df['Year'] = year
        dfs.append(fuel_df)

    df = dfs[0]
    all_years = df.append(dfs[1:]).set_index('Year', 'Operator').sort_index()
    return all_years.reset_index()


def get_electricity_production_hourly_data(include_units=False):
    all_dfs = []

    for fname in sorted(glob.glob('data/energiateollisuus/sähkö/tunti*')):
        print(fname)
        df = pd.read_excel(fname, header=0)
        df['time'] = (pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']]) - timedelta(hours=3))
        df.set_index('time', inplace=True)
        df.index = df.index.tz_localize(pytz.utc).tz_convert(LOCAL_TZ)
        df.drop(columns=['Year', 'Month', 'Day', 'Hour', 'Week'], inplace=True)
        for col in list(df.columns):
            if 'Unnamed' in col or '\n' not in col or 'Net import' in col:
                df.drop(columns=[col], inplace=True)
                continue
            df.rename(columns={col: col.split('\n')[1]}, inplace=True)
        all_dfs.append(df)

    df = pd.concat(all_dfs, sort=True)
    df = df.dropna(subset=['CHP']).drop(columns='CHP')

    return df


def get_electricity_production_fuel_data():
    frames = pd.read_excel('data/energiateollisuus/sähkö/Sähköntuotannon polttoaineet.xlsx', sheet_name=None, header=0)
    dfs = []

    for sheet_name, df in frames.items():
        # Make the dates refer to the end of the month
        df['Day'] = 1
        df['Date'] = pd.to_datetime(df[['Year', 'Month', 'Day']])

        df = df.drop(columns=['Year', 'Month', 'Day', 'Hour', 'Week'])
        df = df.set_index('Date')

        for col in list(df.columns):
            if 'summa' in col or col.startswith('tuotanto') or 'Unnamed' in col:
                df = df.drop(columns=[col])
                continue
            if 'ei-bio' in col:
                df = df.rename(columns={col: col.replace('ei-bio', '').strip()})

        df = pd.melt(df.reset_index(), id_vars=['Date'])
        l = df.variable.str.split(' ')
        df['Method'] = l.map(lambda x: x[0])
        df['Method'] = df['Method'].map(dict(erillistuotanto='Separate Thermal', yhteistuotanto='CHP'))

        df['Fuel'] = l.map(lambda x: x[1])
        df['Fuel'] = df['Fuel'].map(dict(
            hiili='Coal',
            öljy='Oil',
            maakaasu='Natural gas',
            turve='Peat',
            bio='Bio',
            muu='Other'
        ))
        if 'Sähköntuotanto' in sheet_name:
            df.name = 'Production'
        else:
            assert 'Polttoaine-energia' in sheet_name
            df.name = 'FuelUse'

        df = df.rename(columns=dict(value=df.name))
        df = df.drop(columns=['variable'])
        dfs.append(df)

    df = dfs[0].merge(dfs[1], how='outer', on=['Date', 'Method', 'Fuel'])
    df = df.set_index(['Date', 'Method', 'Fuel']).sort_index().reset_index()

    return df


def get_electricity_monthly_data(include_units=False):
    df = pd.read_excel(
        'data/a_Electricity_netproduction_imports_and_exports_(GWh)_in_Finland.xlsx',
        header=[4, 5], index_col=[0, 1, 2, 3]
    )
    df = df.T.iloc[2:].copy()
    df.index = pd.to_datetime(df.index.to_series().apply(lambda x: '%s %s' % (x[1], x[0])))
    df.index = df.index

    # Drop columns with all NaNs
    df = df.dropna(axis=1, how='all')

    # Rename columns to a more usable format
    df.columns = df.columns.to_flat_index()
    col_names = ['/'.join([x for x in y if isinstance(x, str) and x.strip()]) for y in df.columns.to_list()]
    df.columns = pd.Index(col_names)
    # Fix one left import artefact
    df.rename(columns={'TOTAL SUPPLY/Russia': 'TOTAL SUPPLY'}, inplace=True)

    return df


def get_electricity_production_fuel_consumption():
    # https://energia.fi/files/426/Sahkon_hankinta_energialahteittain_2007-2017_web.xls.xlsx
    sheets = pd.read_excel(
        'data/Sahkon_hankinta_energialahteittain_2007-2017_web.xls.xlsx',
        header=[30, 31], index_col=0, sheet_name=None
    )

    all_dfs = None
    for year, df in sheets.items():
        df.columns = pd.Index([' '.join([x for x in y if 'Unnamed' not in x]) for y in df.columns.to_flat_index()])
        df.dropna(how='all', inplace=True)
        df['year'] = int(year)
        if all_dfs is None:
            all_dfs = df
        else:
            all_dfs = all_dfs.append(df)

    df = all_dfs
    df = df.reset_index()

    return df


def update_quilt_datasets():
    import quilt
    from quilt.data.jyrjola import energiateollisuus

    fuel_df = get_district_heating_fuel_stats()
    energiateollisuus._set(['district_heating_fuel'], fuel_df)

    production_df = get_district_heating_production_stats()
    energiateollisuus._set(['district_heating_production'], production_df)

    """
    hourly = get_electricity_production_hourly_data()
    energiateollisuus._set(['electricity_production_hourly'], hourly)

    el_fuel = get_electricity_production_fuel_data()
    energiateollisuus._set(['electricity_production_fuels'], el_fuel)
    """

    quilt.build('jyrjola/energiateollisuus', energiateollisuus)
    quilt.push('jyrjola/energiateollisuus', is_public=True)


if __name__ == '__main__':
    update_quilt_datasets()
