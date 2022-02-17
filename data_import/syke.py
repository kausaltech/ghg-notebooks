import pandas as pd
from utils.dvc import update_dataset


def import_emissions():
    df = pd.read_excel(
        'data/syke/KAIKKI KUNNAT_ALas 1.2_web.xlsx',
        sheet_name='DATA',
        header=0
    )
    drop_cols = [col for col in df.columns if 'Unnamed' in col]
    df = df.drop(columns=drop_cols)
    df.vuosi = df.vuosi.astype(int)
    VAL_COLS = {
        'ktCO2e': 'Gg',
        'ktCO2e_tuuli': 'Gg',
        'energiankulutus (GWh)': 'GWh'
    }
    BOOL_COLS = ['hinku-laskenta', 'päästökauppa']

    for col in df.columns:
        if col in VAL_COLS:
            df[col] = df[col].astype(float).astype('pint[%s]' % VAL_COLS[col])
        elif col in BOOL_COLS:
            df[col] = df[col].map(dict(On=True, Ei=False)).astype(bool)
        else:
            df[col] = df[col].astype('category')

    df = df.rename(columns={'energiankulutus (GWh)': 'energiankulutus'})

    return df


if __name__ == '__main__':
    df = import_emissions()
    update_dataset('syke/alas_emissions', df)
