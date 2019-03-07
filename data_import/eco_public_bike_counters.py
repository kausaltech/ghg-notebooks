import requests
import requests_cache

requests_cache.install_cache(allowable_methods=['GET', 'POST'])

TYPE_MAP = {
    1: 'pedestrian',
    2: 'bicycle',
    12: 'pedestrian_bicycle',
}


def convert_bike_counter(data):
    out = {}
    out['type'] = TYPE_MAP[data.pop('mainPratique')]
    out['name'] = data.pop('nom')
    out['lat'] = float(data.pop('lat'))
    out['lon'] = float(data.pop('lon'))
    out['id'] = data.pop('idPdc')
    out['counters'] = [dict(id=x['id'], type=TYPE_MAP[x['pratique']]) for x in data.pop('pratique')]

    out.update(data)

    return out


def get_counter_values(org_id, counter, type, starts_at, ends_at):
    data = {
        'idOrganisme': org_id,
        'idPdc': counter['id'],
        'starts_at': starts_at.strftime('%m/%d/%Y'),
        'ends_at': ends_at.strftime('%m/%d/%Y'),
    }
    pass


def get_counter_data(locality):
    if locality.lower() != 'helsinki':
        raise Exception("Invalid locality")

    locality_id = 5589

    API_BASE = 'https://www.eco-public.com/ParcPublic'
    resp = requests.post(f'{API_BASE}/GetCounterList', data=dict(id=locality_id))
    data = [convert_bike_counter(x) for x in resp.json()]

    from pprint import pprint
    pprint(data)


if __name__ == '__main__':
    get_counter_data('helsinki')
