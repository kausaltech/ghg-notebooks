import math
import re
from io import BytesIO

import openpyxl
import requests
import pandas as pd
import numpy as np
from utils.dvc import update_dataset


def _load_xlsx_url(url) -> openpyxl.Workbook:
    resp = requests.get(url)
    resp.raise_for_status()
    return openpyxl.load_workbook(BytesIO(resp.content))


EURO_CLASS_YEARS = {
    'EURO 0': (None, 1992),
    'EURO 1': (1993, 1996),
    'EURO 2': (1997, 2000),
    'EURO 3': (2001, 2005),
    'EURO 4': (2006, 2009),
    'EURO 5': (2010, 2014),
    'EURO 6': (2015, None),
}

ROAD_TYPE_MAP = {
    'HA kadut': 'Cars-Urban',
    'HA tiet': 'Cars-Highways',
    'KA kadut': 'Trucks-Urban',
    'KA tiet': 'Trucks-Highways',
    'KAIP tiet': 'Trucks:NoTrailer-Highways',
    'KAP tiet': 'Trucks:Trailer-Highways',
    'KAIP kadut': 'Trucks:NoTrailer-Urban',
    'KAP kadut': 'Trucks:Trailer-Urban',
    'LA kadut ': 'Buses-Urban',
    'LA tiet ': 'Buses-Highways',
    'PA kadut': 'Vans-Urban',
    'PA tiet': 'Vans-Highways',
    'Moottoripyörät': 'Motorcycles-All',
    'Mopoautot': 'Microcars-All',
    'Mopot': 'Mopeds-All',
}

VEHICLE_TYPE_MAP = {
    'HA': 'Cars',
    'PA': 'Vans',
    'LA': 'Buses',
    'KAIP': 'Trucks',
    'KAP': 'Trucks with trailer'
}

ENGINE_TYPE_MAP = {
    'bensiini': 'gasoline',
    'kaasu': 'gas',
    'sähkö PHEV bensiini': 'PHEV (gasoline)',
    'sähkö PHEV diesel': 'PHEV (diesel)',
    'sähkö BEV': 'electric',
    'sähkö': 'electric',
    'vety': 'hydrogen',
}

MAX_YEAR = 2016


def get_liisa_muni_data(year):
    url = 'http://lipasto.vtt.fi/liisa/kunnat%s.xlsx' % year
    if year <= 2016:
        skiprows = 12
    else:
        skiprows = 9
    df = pd.read_excel(url, skiprows=skiprows, header=0)
    # Drop columns with no values
    df.dropna(axis=1, how='all', inplace=True)
    # Drop rows with any non-values
    df.dropna(axis=0, how='any', inplace=True)
    # Rename columns
    columns_left = list(df.columns)
    column_map = {
        columns_left.pop(0): "Municipality",
        columns_left.pop(0): "type",
    }
    while len(columns_left):
        column_name = columns_left.pop(0)
        # Column names are like "kulutus [t]". Split the strings into
        # quantity, unit pairs.
        m = re.search(r'([\w\.\s]+)\[(\w+)\]', column_name)
        if m is None:
            if year <= 2014:
                quantity = column_name
            else:
                raise Exception('Invalid column: %s' % column_name)
        else:
            quantity, unit = m.groups()
        quantity = quantity.strip()
        if quantity == 'CO2 ekv.':
            quantity = 'CO2e'
        elif quantity == 'kulutus':
            quantity = 'FuelConsumption'
        elif quantity == 'energia':
            quantity = 'Energy'
        elif quantity == 'suorite':
            quantity = 'Mileage'
        column_map[column_name] = quantity
        # print("%s [%s]" % (quantity, unit))

    df.rename(index=str, columns=column_map, inplace=True)

    # Filter rows with summaries
    summary_labels = [x for x in df['type'].unique() if 'yhteensä' in x.lower()]
    df = df[~df['type'].isin(summary_labels)]

    df.set_index(['type'], inplace=True)
    mapping = {x: tuple(y.split('-')) for x, y in ROAD_TYPE_MAP.items()}
    df.rename(index=mapping, level='type', inplace=True)

    df.index = pd.MultiIndex.from_tuples(df.index, names=['Vehicle', 'Road'])
    df.set_index(['Municipality'], append=True, inplace=True)
    df = df.reorder_levels(['Municipality', 'Vehicle', 'Road']).sort_index()

    # 'suorite' is in Mkm, convert to km
    df['Mileage'] *= 1000000
    df = df.reset_index()

    if year <= 2014:
        tdf = df[df.Vehicle.isin(['Trucks:NoTrailer', 'Trucks:Trailer'])]\
            .drop(columns='Vehicle').groupby(['Municipality', 'Road']).sum().reset_index()
        tdf['Vehicle'] = 'Trucks'
        df = df.append(tdf, sort=False)
        df = df[~df.Vehicle.isin(['Trucks:NoTrailer', 'Trucks:Trailer'])]
        df = df.set_index(['Municipality', 'Vehicle', 'Road']).sort_index().reset_index()

    return df


def get_mileage_per_engine_type():
    # Returns: percentage of kms per engine type (%)
    url = "http://lipasto.vtt.fi/aliisa/suoritejakaumat.xlsx"
    df = pd.read_excel(url, skiprows=4, header=0, index_col=0, usecols="A:I")

    # Make sure we have parsed the .xlsx correctly
    assert df.index.name == 2019
    df.index.name = 'Vehicle'

    # Remove useless rows from dataset
    df.dropna(axis=0, inplace=True, how='all')
    df.drop(['Yhteensä', 2019], inplace=True)
    # Make sure the keys we're interested in are in the data
    assert 'HA bensiini' in df.index
    assert 'HA diesel' in df.index

    engine_map = {}
    for key in df.index:
        vehicle_type = key.split(' ')[0]
        vehicle_type = VEHICLE_TYPE_MAP[vehicle_type]

        engine_type = ' '.join(key.split(' ')[1:])
        engine_type = ENGINE_TYPE_MAP.get(engine_type, engine_type)
        engine_map[key] = (vehicle_type, engine_type)

    df.rename(index=engine_map, inplace=True)

    df.index = pd.MultiIndex.from_tuples(df.index, names=('Vehicle', 'Engine'))

    col_map = {x: x.upper() for x in df.columns if x.lower().startswith('euro')}
    col_map['Yhteensä'] = 'Sum'
    df.rename(columns=col_map, inplace=True)

    return df


def _parse_gas_section(sections, rows):
    header_row = rows[0]
    assert header_row[0].value == 'Emission standard', "Wrong value: '%s'" % header_row[0].value
    units = {col_idx: cell.value for col_idx, cell in enumerate(header_row[1:]) if cell.value}
    index_tuples = []
    data = []
    # Assume all units are the same
    unit = units[sections[0][0]]

    for row in rows[1:]:
        index_name = row[0].value
        if index_name is None:
            continue
        if not isinstance(index_name, str):
            index_name = str(int(index_name))
        if not index_name:
            continue
        if 'average' in index_name.lower():
            break
        # Try to find two years in the label
        match = re.search(r'(\d{4})[\s-]+(\d{4})', index_name)
        if match:
            start_year, end_year = [int(x) for x in match.groups()]
        else:
            # Check it's of type "2015 -->"
            match = re.search(r'(\d{4}) \-\-\>', index_name)
            if match:
                start_year = int(match.groups()[0])
                end_year = MAX_YEAR
            else:
                # Otherwise it's just for a single yaer
                match = re.match(r'[-> ]*(\d{4})', index_name)
                if not match:
                    continue
                start_year = end_year = int(match.groups()[0])

        for col_idx, section in sections:
            if 'average' in section.lower():
                continue
            val = row[col_idx].value
            if val == '':
                break
            # The sheets have some crazy values for future years
            if start_year > MAX_YEAR:
                break
            assert units[col_idx] == unit
            for year in range(start_year, end_year + 1):
                index_tuples.append((year, section))
                data.append(row[col_idx].value)

    return index_tuples, data


def _parse_unit_emission_excel(book: openpyxl.Workbook):
    sheet = book.worksheets[0]
    rows = list(sheet.rows)
    section_row = rows[5]
    sections = [(col_idx, label.value) for col_idx, label in enumerate(section_row) if label.value]

    gases = ['CO', 'HC', 'NOx', 'PM', 'CH4', 'N2O', 'SO2', 'CO2', 'CO2e']
    gas_sections = {}

    for row_idx, row in enumerate(rows):
        val = row[0].value
        if val not in gases:
            continue
        assert val not in gas_sections
        gas_sections[val] = row_idx

    assert 'CO2e' in gas_sections

    for gas, row_start in list(gas_sections.items()):
        remaining_rows = rows[row_start + 1:]
        index_tuples, data = _parse_gas_section(sections, remaining_rows)

        index = pd.MultiIndex.from_tuples(index_tuples, names=['Year', 'Road'])
        df = pd.DataFrame(data, index=index, columns=[gas])
        road_map = {
            'Highway driving': 'Highways',
            'Urban driving, streets': 'Urban',
        }
        df.rename(index=road_map, level='Road', inplace=True)
        df.sort_index(inplace=True)
        gas_sections[gas] = df

    df_list = []
    for gas in gases:
        if gas not in gas_sections:
            continue
        df_list.append(gas_sections[gas])

    df = pd.concat(df_list, axis=1, sort=False)
    # Move year from index into a column
    df.reset_index(level='Year', inplace=True)
    df.rename(columns={'Year': 'Car year'}, inplace=True)

    for euro_class, (start, end) in EURO_CLASS_YEARS.items():
        if not start:
            df.loc[df['Car year'] <= end, 'Class'] = euro_class
        elif not end:
            df.loc[df['Car year'] >= start, 'Class'] = euro_class
        else:
            df.loc[(df['Car year'] <= end) & (df['Car year'] >= start), 'Class'] = euro_class

    return df


def get_car_unit_emissions():
    # Returns: avg. CO2e per engine type (g/km)
    # sources: VTT Lipasto

    PASSENGER_EMISSIONS = (
        ('habense', 'gasoline'),
        ('hadiese', 'diesel'),
        ('haffve', 'FFV'),
        ('hakaasue', 'gas'),
    )

    df_list = []
    key_list = []
    for fname, engine_type in PASSENGER_EMISSIONS:
        url = 'http://lipasto.vtt.fi/yksikkopaastot/henkiloliikennee/tieliikennee/henkiloautote/%s.xlsx' % fname
        print(url)
        book = _load_xlsx_url(url)
        df = _parse_unit_emission_excel(book)
        df_list.append(df)
        key_list.append(engine_type)

    df = pd.concat(df_list, keys=key_list, names=['Engine'])

    return df

    VALUES = {
        "HA sähkö BEV": {  # battery-powered electric cars
            "HA tiet": 0.20 * 103.6,   # (kWh/km) * (g (CO2e)/kWh) = g/km
            "HA kadut": 0.17 * 103.6,
        }
    }
    index, series = zip(*[((i, j), VALUES[i][j]) for i in VALUES for j in VALUES[i]])
    index = pd.MultiIndex.from_tuples(index, names=['power_source', 'road_type'])
    return pd.DataFrame(list(series), index=index, columns=['emission_factor'])


def get_all_liisa_data():
    dfs = []

    for year in (2012, 2013, 2014, 2015, 2016, 2017, 2018, 2019):
        print(year)
        df = get_liisa_muni_data(year).reset_index()
        df['Year'] = year
        dfs.append(df)

    all_df = dfs[0].append(dfs[1:]).set_index(['Year', 'Municipality']).reset_index()
    return all_df


if __name__ == '__main__':
    import requests_cache
    requests_cache.install_cache('lipasto')

    df = get_car_unit_emissions().reset_index()
    update_dataset('vtt/lipasto/car_unit_emissions', df)

    df = get_all_liisa_data()
    update_dataset('vtt/lipasto/emissions_by_municipality', df)

    df = get_mileage_per_engine_type().reset_index()
    update_dataset('vtt/lipasto/mileage_per_engine_type', df)
