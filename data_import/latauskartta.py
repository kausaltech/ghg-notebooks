import re
from datetime import datetime

import requests
import requests_cache
import pandas as pd
import geopandas
import dateutil.parser
from shapely.geometry import Point


requests_cache.install_cache()


def _process_location(data):
    out = {}
    parts = [x.strip() for x in data['address'].split(',')]
    post = parts.pop()
    out['street_address'] = ', '.join(parts)
    if ' ' not in post:
        out['post_code'] = None
        out['municipality'] = post
    else:
        #print(post)
        post = re.sub(r'\W', ' ', post)
        m = re.match(r'([0-9 ]+)\s+(\w+)', post)
        if m:
            out['post_code'], out['municipality'] = m.groups()
    out['n_chargers'] = data.get('n_chargers', 0)
    out['country'] = data['country']
    out['coords'] = Point(float(data['lon']), float(data['lat']))
    out['name'] = data['title']
    if data['created']:
        out['created_at'] = dateutil.parser.parse(data['created'])

    return out


def get_charging_stations():
    resp = requests.get('https://latauskartta.fi/backend.php?idlimit=0&action=getData&editmode=false&chargers_type2=true&chargers_spc=false&chargers_chademo=true&chargers_ccs=true&chargers_tyomaa=false&unverified=false')
    data = resp.json()
    locations = data['locations']
    for charger in data['chargers']:
        if charger['location'] not in locations:
            continue
        loc = locations[charger['location']]
        loc['n_chargers'] = loc.get('n_chargers', 0) + int(charger['kpl'])

    locations = [_process_location(x) for x in locations.values()]
    df = pd.DataFrame(locations)
    gdf = geopandas.GeoDataFrame(df, geometry='coords')
    return gdf


df = get_charging_stations()
electric_stations = df[df['municipality'] == 'Helsinki']['created_at']
print(sorted(electric_stations))
