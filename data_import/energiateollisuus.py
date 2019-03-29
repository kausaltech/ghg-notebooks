from datetime import timedelta
import pytz
import pandas as pd
import pintpandas  # noqa


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
    "Raskaat öljyt": "1145",  # Raskas polttoöljy, rikkipitoisuus <0,5%
    "Kierrätys- ja jäteöljyt": "116",
    "Muut öljytuotteet": "119",
    "Kivihiili ja antrasiitti": "1212",  # Kivihiili
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


def get_district_heating_fuel_stats(include_units=False):
    df = pd.read_excel('Vuositaulukot17_netti.xlsx', sheet_name='Taul3', header=2)
    df = df.drop(columns=df.columns[0])
    col_map = {
        df.columns[0]: 'producer_id',
        df.columns[1]: 'producer_name',
    }
    df = df.rename(columns=col_map)

    # Drop rows that don't have a producer id
    df = df[~df.producer_id.isnull()]

    df.producer_name = df.producer_name.astype(str)

    # Make sure data columns have numbers in them
    for col in df.columns[2:]:
        df[col] = pd.to_numeric(df[col], errors='coerce')

    # Convert columns into rows
    df = df.melt(id_vars=['producer_id', 'producer_name'], var_name='quantity', value_name='energy')
    # Drop NaN rows
    df = df[~df.energy.isna()]

    df['statfi_fuel_code'] = df.quantity.map(ENERGY_SOURCE_MAP)

    if include_units:
        df.energy = df.energy.astype('pint[GWh]')

    return df


def get_electricity_production_hourly_data(include_units=False):
    df = pd.read_excel('tuntidata_2017.xlsx', header=0)
    df['time'] = (pd.to_datetime(df[['Year', 'Month', 'Day', 'Hour']]) - timedelta(hours=3))
    df.set_index('time', inplace=True)
    df.index = df.index.tz_localize(pytz.utc).tz_convert(LOCAL_TZ)
    df.drop(columns=['Year', 'Month', 'Day', 'Hour', 'Week'], inplace=True)
    for col in list(df.columns):
        if 'Unnamed' in col:
            df.drop(columns=[col], inplace=True)
            continue
        if include_units:
            df[col] = df[col].astype('pint[MWh]')
        df.rename(columns={col: col.split('\n')[1]}, inplace=True)
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

    if include_units:
        for col_name in df.columns:
            df[col_name] = df[col_name].astype('pint[GWh]')

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
