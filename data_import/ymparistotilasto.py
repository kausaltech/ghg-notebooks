import json
import os
import re
from datetime import datetime, timedelta

import pandas as pd
import quilt
import requests
from pandas_pcaxis import PxParser
from pandas_pcaxis.pxweb_api import PXWebAPI

from utils.dvc import update_dataset_from_px


def update_topic(api, path):
    topics = api.list_topics('/'.join(path))
    for e in topics:
        subpath = path + [e['id']]
        if e['type'] == 'l':
            update_topic(api, subpath)
        else:
            assert e['type'] == 't'
            print(e['updated'].isoformat(), e['id'])
            continue
            str_path = '/'.join(subpath)
            pxf = api.get_table(str_path)
            dataset_identifier = str_path.lower().replace('.px', '')
            update_dataset_from_px(dataset_identifier, pxf)


if __name__ == '__main__':
    import requests_cache
    requests_cache.install_cache('ymparistotilastot')

    api = PXWebAPI('http://api.aluesarjat.fi', 'fi')
    base_path = 'Ympäristötilastot'
    update_topic(api, [base_path])
