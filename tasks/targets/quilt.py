import luigi
import quilt
from quilt.imports import _from_core_node
from quilt.tools.store import PackageStore
from quilt.tools.command import HTTPResponseException
from utils.quilt import pint_df_to_quilt, quilt_to_pint_df

import settings


class QuiltDataframeTarget(luigi.Target):
    def __init__(self, package_name, sub_path, timestamp=None):
        if '/' not in package_name:
            package_name = '/'.join([settings.QUILT_USER, package_name])
        self.package_name = package_name
        try:
            quilt.install(self.package_name, force=True)
        except HTTPResponseException:
            pass

        self.sub_path = sub_path
        self.timestamp = timestamp

    def find_package(self):
        parts = self.package_name.split('/')
        store, package = PackageStore.find_package(None, parts[0], parts[1])
        return store, package

    def full_path(self):
        path = self.package_name
        if self.sub_path:
            path += '/' + self.sub_path
        return path

    def exists(self):
        store, package = self.find_package()
        if not package:
            return False

        root_node = _from_core_node(store, package)
        data_node = getattr(root_node, self.sub_path, None)
        if data_node is None:
            return False

        df = data_node()
        if df is None:
            return False

        if not self.timestamp:
            return True

        if df.index.dtype.kind != 'M':
            raise Exception('Quilt package %s index is not a timestamp' % self.full_path())

        if (df.index >= self.timestamp).any():
            return True
        else:
            return False

    def _get_root_node(self):
        store, package = self.find_package()
        if not package:
            quilt.build(self.package_name)
            store, package = self.find_package()

        root_node = _from_core_node(store, package)
        return root_node

    def update(self, df):
        root_node = self._get_root_node()

        df, meta = pint_df_to_quilt(df)
        root_node._set([self.sub_path], df)
        data_node = getattr(root_node, self.sub_path)
        data_node._meta.update(meta)

        quilt.build(self.package_name, root_node)

    def merge(self, df):
        root_node = self._get_root_node()

        data_node = getattr(root_node, self.sub_path, None)
        if data_node is None:
            return self.update(df)

        old_df = quilt_to_pint_df(data_node)
        merged = old_df.append(df).sort_index()
        # Remove duplicate rows
        merged = merged[~merged.index.duplicated(keep='first')]

        merged, meta = pint_df_to_quilt(merged)

        root_node._set([self.sub_path], merged)
        data_node = getattr(root_node, self.sub_path)
        data_node._meta.update(meta)

        quilt.build(self.package_name, root_node)

        return merged

    def push(self):
        quilt.push(self.package_name, is_public=True)
