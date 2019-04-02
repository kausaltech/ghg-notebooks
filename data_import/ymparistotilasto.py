import os
import csv
from datetime import datetime, timedelta

import pandas as pd
from collections import OrderedDict
import quilt
from pandas_pcaxis import PxParser

from utils.quilt import update_node_from_pcaxis


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


if __name__ == '__main__':
    update_quilt('jyrjola/ymparistotilastot')
