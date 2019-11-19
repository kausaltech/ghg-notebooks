import re
import os
import csv
from datetime import datetime, timedelta

import pandas as pd
import requests
from collections import OrderedDict
import quilt
from pandas_pcaxis import PxParser

from utils.quilt import update_node_from_pcaxis

import requests_cache

requests_cache.install_cache()


def update_quilt(quilt_path):
    import glob
    import settings

    CATEGORIES = {
        'J': 'Jate',
        'I': 'Ilmanlaatu',
        'L': 'Liikenne',
        'V': 'Vedet',
        'M': 'Maankaytto',
        'T': 'Ymparistotalous',
        'E': 'Energia',
    }

    def upload_px_dataset(root_node, file):
        fname = os.path.splitext(os.path.basename(file))[0].lower()
        letter = fname[0]
        if letter != 'e':
            return

        content = open(file, 'r', encoding='windows-1252').read()
        parser = PxParser()
        file = parser.parse(content)
        now = datetime.now()

        if 'last_updated' not in file.meta or (now - file.meta['last_updated']) > timedelta(days=2 * 365):
            return

        print(fname)
        if root_node:
            quilt_target = root_node
        else:
            quilt_target = quilt_path

        node = update_node_from_pcaxis(quilt_target, fname, file)
        return node

    SKIP_FILES = ['A4_kopiopaperi.px']

    data_dir = os.path.join(settings.DATA_DIR, 'ymp')
    files = glob.glob('%s/*.px' % data_dir)
    skip_until = None

    root_node = None
    for file in files:
        if skip_until:
            if skip_until not in file:
                continue
            skip_until = None

        skip = False
        for sf in SKIP_FILES:
            if sf in file:
                skip = True
                break
        if skip:
            continue

        ret = upload_px_dataset(root_node, file)
        if ret:
            root_node = ret

    assert root_node
    quilt.build(quilt_path, root_node)
    quilt.push(quilt_path, is_public=True)


def scrape_one(dbname):
    print('%s' % dbname)
    dbname = dbname.replace(' ', '*;').replace('Ä', '*196;').replace('Ö', '*214;')
    url = 'http://www.aluesarjat.fi/graph/GraphList.aspx?Path=..%s&Matrix=&Gsave=false&Gedit=false&Case=DTD' % dbname
    print(url)
    resp = requests.get(url)
    resp.raise_for_status()
    s = resp.text
    dbs = []
    expr = r'file=\.\.%s(.+?\.px)"' % dbname.replace('*', r'\*')
    for match in re.finditer(expr, s):
        m = match.groups()[0]
        print('\t%s' % m)
        dbs.append('..%s/%s' % (dbname, m))
    return dbs


def scrape_all_px_urls():
    url = 'http://www.helsinginymparistotilasto.fi/graph/style/YMP/menu.aspx?ssid=1910091232174&IsGedit=false&Mpar=&Msel=&Mtype=&Langcode=FI'
    resp = requests.get(url)
    resp.raise_for_status()

    s = resp.content.decode('utf8')
    dbs = []
    for match in re.finditer(r'\'(/DATABASE/\w.+?)\'', s):
        g = match.groups()[0]
        dbs += scrape_one(g)
        break

    urls = []
    for db in dbs:
        url = 'http://www.helsinginymparistotilasto.fi/graph/Download.aspx?file=%s' % db
        urls.append(url)
    return urls


if __name__ == '__main__':
    urls = scrape_all_px_urls()
    print(urls)
    # update_quilt('jyrjola/ymparistotilastot')
