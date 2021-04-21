import pandas as pd


URL = 'https://www.hsy.fi/fi/asiantuntijalle/ilmastonmuutos/hillinta/seuranta/Documents/Paakaupunkiseudun_KHK-paastot_1990_ja_2000-2017.xlsx'


def read_hsy_emissions():
    REQUIRED_LEVELS = {'Kaupunki', 'Vuosi', 'Sektori1', 'Sektori2', 'Sektori3', 'Sektori4'}
    REQUIRED_COLS = {'Energiankulutus', 'Päästöt'}

    URL = 'data/hsy_khk_2019.xlsx'
    df = pd.read_excel(URL, index_col=[0, 1, 2, 3, 4, 5])
    assert REQUIRED_LEVELS.issubset(set(df.index.names))
    assert REQUIRED_COLS.issubset(set(df.columns))

    df.drop('PKS', level='Kaupunki', inplace=True)
    drop_cols = [col for col in df.columns if 'Unnamed' in col]
    df = df.drop(columns=drop_cols)

    return df


if __name__ == '__main__':
    from utils.dvc import update_dataset

    df = read_hsy_emissions().reset_index()

    update_dataset('hsy/pks_khk_paastot', df)
