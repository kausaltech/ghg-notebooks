import logging
from pintpandas.pint_array import PintType


logger = logging.getLogger(__name__)


def pint_df_to_quilt(df):
    meta = {}
    copied = False
    for col_name, dtype in list(df.dtypes.items()):
        if not isinstance(dtype, PintType):
            continue
        if not copied:
            df = df.copy()
            copied = True
        df[col_name] = df[col_name].pint.m
        meta['%s_unit' % col_name] = str(dtype.units)

    return df, meta


def quilt_to_pint_df(node):
    meta = node._meta
    df = node()
    for key, unit in meta.items():
        if not key.endswith('_unit'):
            continue
        key = key.split('_unit')[0]
        if key not in df.columns:
            logger.warning('column %s not found in dataframe' % key)
            continue
        df[key] = df[key].astype('pint[%s]' % unit)
    return df
