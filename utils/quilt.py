import tempfile
import threading
import quilt
import fastparquet
from quilt.tools import store
from quilt.tools.command import _materialize
from quilt.imports import _from_core_node

import importlib
import logging
import collections
from datetime import datetime


logger = logging.getLogger(__name__)


quilt_lock = threading.Lock()


def _load_from_quilt(package_path):
    user, root_pkg, *sub_paths = package_path.split('/')

    pkg_store, root_node = store.PackageStore.find_package(None, user, root_pkg)
    if root_node is None:
        quilt.install(package_path, force=True)
        pkg_store, root_node = store.PackageStore.find_package(None, user, root_pkg)

    node = root_node
    while len(sub_paths):
        name = sub_paths.pop(0)
        for child_name, child_node in node.children.items():
            if child_name != name:
                continue
            try:
                node = _from_core_node(pkg_store, child_node)
            except store.StoreException:
                quilt.install(package_path, force=True)
                node = _from_core_node(pkg_store, child_node)
            break
        else:
            raise Exception('Dataset %s not found' % package_path)
    return node


def load_datasets(packages, include_units=False):
    if not isinstance(packages, (list, tuple)):
        packages = [packages]

    datasets = []
    for package_path in packages:
        with quilt_lock:
            node = _load_from_quilt(package_path)

        try:
            df = node()
        except store.StoreException:
            with quilt_lock:
                _materialize(node)
            df = node()

        if isinstance(df, str):
            pf = fastparquet.ParquetFile(df)
            df = pf.to_pandas()

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
    except Exception:
        from pprint import pprint
        pprint(meta)
        raise

    getattr(root_node, sub_path)._meta['pxmeta'] = meta

    return root_node


def df_to_quilt(df, path):
    parts = path.split('/')
    assert len(parts) > 2

    root_pkg = '/'.join(parts[0:2])
    try:
        quilt.install(root_pkg, force=True)
    except Exception:
        pass

    object_encoding = {}
    df = df.copy()
    for col, dtype in df.dtypes.iteritems():
        if dtype.name in ('Int8', 'Int32'):
            object_encoding[col] = 'int32'
            df[col] = df[col].astype(object)
        else:
            object_encoding[col] = 'infer'

    with tempfile.NamedTemporaryFile(suffix='.parquet') as f:
        print('writing to %s' % f.name)
        fastparquet.write(f.name, df, compression='snappy', object_encoding=object_encoding)
        print('build')
        quilt.build(path, f.name)
        print('push')
        quilt.push(root_pkg, is_public=True)
