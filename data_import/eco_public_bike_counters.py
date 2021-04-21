from datetime import date, datetime

import pandas as pd
import requests

import requests_cache

requests_cache.install_cache(allowable_methods=['GET', 'POST'])

TYPE_MAP = {
    1: 'pedestrian',
    2: 'bicycle',
    12: 'pedestrian_bicycle',
}


def parse_date(s, reverse=False):
    if s == 'null':
        return None
    if reverse:
        fmt = '%m/%d/%Y'
    else:
        fmt = '%d/%m/%Y'
    return datetime.strptime(s, fmt)


def convert_bike_counter(data, locality_id):
    out = {}
    out['type'] = TYPE_MAP[data.pop('mainPratique')]
    out['name'] = data.pop('nom')
    out['lat'] = float(data.pop('lat'))
    out['lon'] = float(data.pop('lon'))
    out['id'] = data.pop('idPdc')
    out['counters'] = [dict(id=x['id'], type=TYPE_MAP[x['pratique']]) for x in data.pop('pratique')]
    out['start_date'] = parse_date(data.pop('debut'))
    out['end_date'] = parse_date(data.pop('fin'))
    out['locality_id'] = locality_id

    out['extra'] = data.copy()

    return out


def get_counter_values(counter, start_date, end_date):
    data = {
        'idOrganisme': counter['locality_id'],
        'idPdc': counter['id'],
        'debut': start_date.strftime('%d/%m/%Y'),
        'fin': end_date.strftime('%d/%m/%Y'),
        'interval': 4,
        'pratiques': ';'.join([counter['id']])  # FIXME
    }
    API_BASE = 'https://www.eco-public.com/ParcPublic'
    resp = requests.post(f'{API_BASE}/CounterData', data=data)
    resp.raise_for_status()
    out = [dict(date=parse_date(x[0], reverse=True), value=int(x[1])) for x in resp.json()[:-1]]
    return out


def get_counters(locality):
    if locality.lower() != 'helsinki':
        raise Exception("Invalid locality")

    locality_id = 5589

    API_BASE = 'https://www.eco-public.com/ParcPublic'
    resp = requests.post(f'{API_BASE}/GetCounterList', data=dict(id=locality_id))
    resp.raise_for_status()
    data = [convert_bike_counter(x, locality_id) for x in resp.json()]

    return data


NAME_MAP = {
    'Auroran silta': 'Auroransilta',
    'Eteläesplanadi': 'Eteläesplanadi',
    'Huopalahti': 'Huopalahti (asema)',
    'Kaisaniemi': 'Kaisaniemi/Eläintarhanlahti',
    'Kaivokatu': 'Kaivokatu',
    'Kulosaaren silta et.': 'Kulosaaren silta et.',
    'Kulosaaren silta po.': 'Kulosaaren silta po.',
    'Kuusisaarentie': 'Kuusisaarentie',
    'Käpylä': 'Käpylä - Pohjoisbaana',
    'Lauttasaarentie': 'Lauttasaaren silta eteläpuoli',
    'Porkkalankatu': 'Lauttasaaren silta pohjoispuoli',
    'Merikannontie': 'Merikannontie',
    'Munkkiniemi ET': 'Munkkiniemen silta eteläpuoli',
    'Munkkiniemi PO': 'Munkkiniemi silta pohjoispuoli',
    'Ooppera': 'Heperian puisto/Ooppera',
    'Pitkäsilta/IT': 'Pitkäsilta itäpuoli',
    'Pitkäsilta/LÄ': 'Pitkäsilta länsipuoli',
    'Ratapihantie': 'Ratapihantie',
    'Viikki': 'Viikintie',
}

if __name__ == '__main__':
    d = get_counters('helsinki')
    d = filter(lambda x: x['type'] == 'bicycle', d)
    d = sorted(d, key=lambda x: -int(x['extra']['total']))

    NAMES = '''
    Ooppera
    Merikannontie
    Kulosaaren silta po.
    #Porkkalankatu
    Pitkäsilta/IT
    Ratapihantie
    Munkkiniemi ET
    Huopalahti
    Lauttasaarentie
    Kuusisaarentie
    Kaisaniemi
    Pitkäsilta/LÄ
    Munkkiniemi PO
    #Kulosaaren silta et.?
    '''

    names = [x.strip() for x in NAMES.strip().splitlines()]
    out = []

    for c in d:
        print(c['name'])
        #if c['name'] not in names:
        #    continue
        """
        print('%-30s %10s %10s %s' % (c['name'], c['start_date'], c['type'], c['extra']['total']))
        for year in (2017, 2018, 2019, 2020):
            vals = get_counter_values(c, date(year, 1, 1), date(year, 12, 31))
            out += [dict(name=c['name'], **x) for x in vals]
            # print('\t%d: %s' % (year, ','.join([str(x['value']) for x in vals])))
        """

    df = pd.DataFrame.from_records(out)
    df.to_parquet('out.pq')
