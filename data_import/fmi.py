import io

import pandas as pd
import requests


URL_FORMAT = 'https://cdn.fmi.fi/weather-observations/products/heating-degree-days/lammitystarveluvut-{year}.utf8.csv'


def _get_heating_degree_days_excel():
    URL = 'https://cdn.fmi.fi/legacy-fmi-fi-content/documents/climate/hdd_1995-2007.xlsx'
    COLS = ('Kunta', 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'Yhteensä')
    KUNTA_MAP = {
        "jomala": "Maarianhamina",
        "helsinki-vantaa": "Vantaa",
        "helsinki kaisaniemi": "Helsinki",
        "tampere-pirkkalan": "Tampere",
        "inari/ivalo": "Ivalo",
    }
    resp = requests.get(URL)
    resp.raise_for_status()
    f = io.BytesIO(resp.content)
    sheets = pd.read_excel(f, sheet_name=None, header=1, names=COLS)
    all_dfs = []
    for sheet_name, df in sheets.items():
        try:
            year = int(sheet_name)
        except ValueError:
            continue
        df = df.dropna().copy()
        df['Kunta'] = df.Kunta.map(lambda x: KUNTA_MAP.get(x.lower(), x.lower().capitalize()))
        df['Vuosi'] = year
        df = df.set_index('Vuosi').reset_index()
        all_dfs.append(df)
    return all_dfs


def get_heating_degree_days():
    dfs = _get_heating_degree_days_excel()
    for year in range(2008, 2018):
        url = URL_FORMAT.format(year=year)
        resp = requests.get(url)
        resp.raise_for_status()
        f = io.BytesIO(resp.content)
        df = pd.read_csv(f)
        df = df.rename(columns={df.columns[0]: 'Kunta'})
        df['Yhteensä'] = df['Vuosi']
        df['Vuosi'] = year
        df = df.set_index('Vuosi').reset_index()
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True, sort=False)
    return df


if __name__ == '__main__':
    import quilt

    USER = 'jyrjola'
    PACKAGE_BASE = 'fmi'

    def build_and_push(package, df):
        quilt.build('%s/%s/%s' % (USER, PACKAGE_BASE, package), df)
        quilt.push('%s/%s/%s' % (USER, PACKAGE_BASE, package), is_public=True)

    df = get_heating_degree_days()
    build_and_push('heating_degree_days', df)
