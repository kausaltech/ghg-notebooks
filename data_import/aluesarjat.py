import re
import requests
import quilt
from datetime import datetime, timedelta
from pandas_pcaxis import PxParser

from utils.quilt import update_node_from_pcaxis


def scrape_one(dbname):
    print('%s' % dbname)
    dbname = dbname.replace(' ', '*;').replace('Ä', '*196;').replace('Ö', '*214;')
    url = 'http://www.aluesarjat.fi/graph/GraphList.aspx?Path=..%s&Matrix=&Gsave=false&Gedit=false&Case=DTD' % dbname
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
    url = 'http://www.aluesarjat.fi/graph/style/HEL/menu.aspx?IsGedit=false&Mpar=&Msel=&Mtype=&Langcode=FI'
    resp = requests.get(url)
    resp.raise_for_status()

    s = resp.content.decode('utf8')
    dbs = []
    for match in re.finditer(r'\'(/DATABASE/\w.+?)\'', s):
        g = match.groups()[0]
        dbs += scrape_one(g)

    urls = []
    for db in dbs:
        url = 'http://www.aluesarjat.fi/graph/Download.aspx?file=%s' % db
        urls.append(url)
    return urls


def download_all_datasets():
    import os
    import settings

    SKIP_URLS = [
        "http://www.aluesarjat.fi/graph/Download.aspx?file=../DATABASE/ALUESARJAT_KAUPUNKIVERKKO/VAESTO_SAL/VAESTOENNUSTEET_SAL/B01ESPS_Vaestoennuste.px"
    ]
    data_dir = os.path.join(settings.DATA_DIR, 'aluesarjat')

    urls = scrape_all_px_urls()
    print(urls)
    for url in urls:
        if url in SKIP_URLS:
            continue

        fname = os.path.join(data_dir, url.split('/')[-1])
        print(fname)
        if os.path.exists(fname):
            continue

        print("downloading")
        resp = requests.get(url)
        resp.raise_for_status()
        with open(fname, 'wb') as f:
            f.write(resp.content)


def update_quilt(quilt_path):
    import os
    import glob
    import settings

    def upload_px_dataset(root_node, file):
        fname = os.path.splitext(os.path.basename(file))[0]
        if 'hginseutu' not in fname.lower() and 'UM' not in fname and 'hki' not in fname.lower():
            return

        print(fname)
        if re.match('^[0-9]', fname):
            # If the name begins with a number, prefix it with an letter
            # to make it a legal Python identifier.
            fname = 'z' + fname

        fname = fname.replace('-', '_').lower()

        content = open(file, 'r', encoding='windows-1252').read()
        parser = PxParser()
        try:
            file = parser.parse(content)
        except Exception as e:
            print(e)
            return

        now = datetime.now()

        parser = PxParser()
        file = parser.parse(content)
        now = datetime.now()
        from pprint import pprint
        #if 'last_updated' not in file.meta or (now - file.meta['last_updated']) > timedelta(days=2 * 365):
        #    return

        print("\t%s" % file.meta['contents'])

        if root_node:
            quilt_target = root_node
        else:
            quilt_target = quilt_path

        node = update_node_from_pcaxis(quilt_target, fname, file)
        return node

    SKIP_FILES = []

    data_dir = os.path.join(settings.DATA_DIR, 'aluesarjat')
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


if __name__ == '__main__':
    #download_all_datasets()
    update_quilt('jyrjola/aluesarjat')
