import pandas as pd


URL = 'https://www.hsy.fi/fi/asiantuntijalle/ilmastonmuutos/hillinta/seuranta/Documents/Paakaupunkiseudun_KHK-paastot_1990_ja_2000-2017.xlsx'


def read_hsy_emissions():
    REQUIRED_LEVELS = {'Kaupunki', 'Vuosi', 'Sektori1', 'Sektori2', 'Sektori3', 'Sektori4'}
    REQUIRED_COLS = {'Energiankulutus', 'Päästöt'}

    df = pd.read_excel(URL, index_col=[0, 1, 2, 3, 4, 5])
    assert REQUIRED_LEVELS.issubset(set(df.index.names))
    assert REQUIRED_COLS.issubset(set(df.columns))

    df.drop('PKS', level='Kaupunki', inplace=True)

    return df


if __name__ == '__main__':
    import quilt

    df = read_hsy_emissions().reset_index()
    quilt.build('jyrjola/hsy/pks_khk_paastot', df)
    quilt.push('jyrjola/hsy/pks_khk_paastot', is_public=True)
