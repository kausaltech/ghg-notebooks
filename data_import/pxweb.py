import json
import os
import time
from datetime import datetime, timedelta

import pandas as pd

import requests_cache
from pandas_pcaxis import PxParser
from pandas_pcaxis.pxweb_api import PXWebAPI
from slugify import slugify

requests_cache.install_cache('pxweb')


def slugify_px_id(px_id):
    return slugify(px_id)


class PXDownloader:
    def __init__(self, api, base_dir):
        self.api = api
        self.base_dir = base_dir

    def download_table(self, file_path, topic_path, topic):
        self.tables.append({
            'file': file_path,
            'topic_path': topic_path,
        })
        try:
            file_updated = datetime.fromtimestamp(os.path.getmtime(file_path))
            if topic['updated'] - file_updated < timedelta(days=1):
                # FIXME
                return
        except FileNotFoundError:
            pass

        print(topic_path)

        dirname = os.path.dirname(file_path)
        if not os.path.exists(dirname):
            os.makedirs(dirname)

        contents = self.api.get_raw_table(topic_path)

        with open(file_path, 'wb') as outf:
            outf.write(contents)

        dt_epoch = topic['updated'].timestamp()
        os.utime(file_path, (dt_epoch, dt_epoch))

    def parse_table(self, fname):
        parser = PxParser()
        with open(fname, 'rb') as inf:
            pxf = parser.parse(inf.read())

    def download_topic_recursive(self, dirname, topic_base_path, topic):
        if topic['type'] == 't':
            assert topic['id'].endswith('.px')
            slugified = slugify_px_id(topic['id'].split('.px')[0])
            fname = f'{dirname}/{slugified}.px'
            self.download_table(fname, f"{topic_base_path}/{topic['id']}", topic)
            # self.parse_table(fname)
        elif topic['type'] == 'l':
            slugified = slugify_px_id(topic['id'])
            topic_path = f"{topic_base_path}/{topic['id']}"
            topics = api.list_topics(topic_path)
            for child_topic in topics:
                self.download_topic_recursive(f'{dirname}/{slugified}', topic_path, child_topic)
        else:
            raise Exception('Invalid topic type: %s' % topic['type'])

    def download_databases(self, only_db=None):
        self.tables = []
        dbs = self.api.list_databases()
        for db in dbs:
            if only_db:
                if db['dbid'] != only_db:
                    continue
            for topic in self.api.list_topics(db['dbid']):
                slugified = slugify_px_id(db['dbid'])
                dirname = f'{self.base_dir}/{slugified}'
                self.download_topic_recursive(dirname, db['dbid'], topic)


if __name__ == '__main__':
    api = PXWebAPI('http://api.aluesarjat.fi', 'fi')
    downloader = PXDownloader(api, 'data/aluesarjat_px')
    #downloader.download_databases(only_db='Ympäristötilastot')
    #with open('data/aluesarjat_px/ymparistotilastot.json', 'w', encoding='utf8') as outf:
    #    json.dump(downloader.tables, outf, ensure_ascii=False, indent=4)

    downloader.download_databases(only_db='Helsingin seudun tilastot')
    with open('data/aluesarjat_px/aluesarjat.json', 'w', encoding='utf8') as outf:
        json.dump(downloader.tables, outf, ensure_ascii=False, indent=4)
