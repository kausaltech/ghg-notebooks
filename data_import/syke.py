from __future__ import annotations

import dataclasses
import datetime
from pathlib import Path
from typing import cast
from pint import UnitRegistry, get_application_registry

import polars as pl
from dvc_pandas.dataset import Dataset, DatasetMeta

from utils.dvc import get_repo

FNAME = Path('~/data/syke/KAIKKI KUNNAT_ALas 1.6_1990_2005-2023e_FINAL2.xlsx')
MODIFIED_AT = datetime.datetime(2025, 1, 16, tzinfo=datetime.UTC)

VAL_COLS = {
    'ktCO2e': 'Gg/a',
    'ktCO2e_tuuli': 'Gg/a',
    'energiankulutus (GWh)': 'GWh/a',
}
BOOL_COLS = ['hinku-laskenta', 'päästökauppa']
MUNI_NUM_COL = 'kuntanumero'
YEAR_COL = 'vuosi'

def import_emissions():
    df = pl.read_excel(
        FNAME,
        sheet_name='DATA',
        has_header=True,

    )
    drop_cols = [col for col in df.columns if 'Unnamed' in col]
    if drop_cols:
        df = df.drop(drop_cols)

    with_cols: list[pl.Expr] = []
    remaining_cols = list(df.columns)

    with_cols.append(pl.col(YEAR_COL).cast(pl.Int32))
    remaining_cols.remove(YEAR_COL)

    with_cols.append(pl.col(MUNI_NUM_COL).cast(pl.Int32))
    remaining_cols.remove(MUNI_NUM_COL)

    units: dict[str, str] = {}
    ureg: UnitRegistry = cast(UnitRegistry, get_application_registry().get())

    for col, unit in VAL_COLS.items():
        col_def = pl.col(col).cast(pl.Float32)
        remaining_cols.remove(col)
        if col == 'energiankulutus (GWh)':
            col = 'energiankulutus'  # noqa: PLW2901
            col_def = col_def.alias('energiankulutus')
        units[col] = str(ureg.parse_units(unit))
        with_cols.append(col_def)

    index_cols = [YEAR_COL]
    for col in BOOL_COLS:
        with_cols.append(pl.col(col).replace_strict(['On', 'Ei'], [True, False], return_dtype=pl.Boolean))
        index_cols.append(col)
        remaining_cols.remove(col)

    df = df.with_columns(with_cols)

    for col in remaining_cols:
        col_def = pl.col(col).cast(pl.Categorical)
        df = df.with_columns(col_def)
        index_cols.append(col)

    # Ahvenanmaa has duplicate rows for some reason
    df = df.filter(pl.col('maakunta') != 'Ahvenanmaa')

    ldf = df.lazy()
    dupes = ldf.group_by(index_cols).agg(pl.len()).filter(pl.col('len') > 1).collect()
    if not dupes.is_empty():
        print('duplicate rows:')
        for col in index_cols:
            print(f'{col}: {"; ".join(dupes[col].unique().sort())}')

    meta = DatasetMeta(
        identifier='syke/alas_emissions',
        units=units,
        index_columns=index_cols,
        modified_at=MODIFIED_AT,
    )
    return df, meta


def save_emissions(df: pl.DataFrame, meta: DatasetMeta):
    df = df.with_columns(pl.col('kunta').cast(pl.Utf8).str.to_lowercase().alias('muni'))
    munis: list[str] = list(df['muni'].unique().sort())
    repo = get_repo()

    ds = Dataset(df, meta=meta)
    repo.add(ds)
    repo.push()
    return

    for muni_id in munis:
        identifier = f'syke/alas_emissions/{muni_id}'
        print(f'Adding {identifier}')
        muni_df = df.filter(pl.col('muni') == muni_id).drop('muni')
        meta = dataclasses.replace(meta, identifier=identifier)
        ds = Dataset(muni_df, meta=meta)
        repo.add(ds)
    repo.push()


if __name__ == '__main__':
    _ = pl.Config.set_tbl_cols(20)
    repo = get_repo()
    df, meta = import_emissions()
    print(df)
    save_emissions(df, meta)
    #update_dataset('syke/alas_emissions', df)
