import quilt
import importlib
import logging
import collections
from datetime import datetime
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


def load_datasets(packages, include_units=False):
    if not isinstance(packages, (list, tuple)):
        packages = [packages]

    datasets = []
    for package_path in packages:
        user, root_pkg, *sub_paths = package_path.split('/')

        mod_path = '%s.%s' % (user, root_pkg)
        try:
            root_mod = importlib.import_module('quilt.data.%s' % mod_path)
        except ImportError:
            quilt.install(package_path)
            root_mod = importlib.import_module('quilt.data.%s' % mod_path)

        mod = root_mod
        if sub_paths:
            for sub_path in sub_paths:
                mod = getattr(mod, sub_path)

        if not isinstance(mod, quilt.nodes.DataNode):
            raise Exception('Invalid package name: %s' % package_path)

        df = mod()
        if include_units:
            for col_name in df.columns:
                unit = mod._meta.get('%s_unit' % col_name, None)
                if not unit:
                    continue
                df[col_name] = df[col_name].astype('pint[%s]' % unit)

        datasets.append(df)

    if len(datasets) == 1:
        return datasets[0]

    return datasets


def update_node_from_pcaxis(root_node_or_path, sub_path, px_file):
    assert '/' not in sub_path

    if isinstance(root_node_or_path, str):
        root_path = root_node_or_path
        root_mod_path = root_path.replace('/', '.')
        try:
            root_node = importlib.import_module('quilt.data.%s' % root_mod_path)
        except ImportError:
            quilt.build(root_path)
            root_node = importlib.import_module('quilt.data.%s' % root_mod_path)
    else:
        root_node = root_node_or_path

    df = px_file.to_df(melt=True, dropna=True)
    root_node._set([sub_path], df)
    meta = dict(px_file.meta)
    for key, val in meta.items():
        if isinstance(val, collections.OrderedDict):
            meta[key] = dict(val)
        elif isinstance(val, datetime):
            meta[key] = val.isoformat()

    try:
        import json
        json.dumps(meta, sort_keys=True)
    except:
        from pprint import pprint
        pprint(meta)
        raise

    getattr(root_node, sub_path)._meta['pxmeta'] = meta

    return root_node
