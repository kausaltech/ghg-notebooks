import re
import os
import json
import time
import random
from datetime import date, timedelta, datetime
from pprint import pprint

import pandas as pd
import requests
import requests_cache
import pytz


#requests_cache.install_cache('nuuka')

_api_base_url = None
_api_key = None


LOCAL_TZ = pytz.timezone('Europe/Helsinki')


def api_get(path, req_params=None):
    assert _api_key and _api_base_url

    params = {}
    if req_params:
        params.update(req_params)
    params['$token'] = _api_key
    params['$format'] = 'json'

    resp = requests.get('%s%s/' % (_api_base_url, path), params=params)
    resp.raise_for_status()
    return resp.json()


def set_api_params(base_url, api_key):
    global _api_base_url
    global _api_key

    _api_base_url = base_url
    _api_key = api_key


def get_measurement_data(building_id, data_point_ids, start_date, end_date):
    params = dict(Building=building_id, DataPointIDs=';'.join([str(x) for x in data_point_ids]))
    params['StartTime'] = start_date.isoformat()
    params['EndTime'] = end_date.isoformat()
    params['TimestampTimeZone'] = 'UTC'
    print(data_point_ids)
    resp = api_get('GetMeasurementDataByIDs', params)
    out = []
    for d in resp:
        #assert d['Name'] is None
        if 'Target' not in d:
            pprint(d)
        assert d['Target'] is None
        dt = datetime.strptime(d['Timestamp'], "%Y-%m-%dT%H:%M:%S")
        dt = pytz.utc.localize(dt)
        dt = dt.astimezone(LOCAL_TZ)
        val = d['Value']
        dpid = d['DataPointID']
        out.append(dict(building_id=building_id, sensor_id=dpid, time=dt, value=val))

    df = pd.DataFrame.from_records(out)
    return df


def get_daily_measurement_data(building_id, data_point_ids, start_date, end_date):
    params = dict(Building=building_id, DataPointIDs=';'.join([str(x) for x in data_point_ids]))
    params['StartTime'] = start_date.isoformat()
    params['EndTime'] = end_date.isoformat()
    params['TimestampTimeZone'] = 'UTC'
    resp = api_get('GetDailyMeasurementData', params)
    out = []
    for d in resp:
        dt = datetime.strptime(d['Timestamp'], "%Y-%m-%dT%H:%M:%S")
        dt = pytz.utc.localize(dt)
        dt = dt.astimezone(LOCAL_TZ)
        val = d['Value']
        dpid = d['DataPointID']
        unit = d.get('Unit')
        d = dict(sensor_id=dpid, time=dt, value=val)
        if unit:
            d['unit'] = unit
        out.append(d)

    df = pd.DataFrame.from_records(out)
    return df


def get_measurement_meta(building_id):
    params = dict(BuildingID=building_id, MeasurementSystem='SI')
    measurements = api_get('GetMeasurementInfo', params)
    out = []
    for m in measurements:
        category = m['Category']
        unit = m['Unit']
        if unit is not None:
            if category in ('electricity', 'heating'):
                assert unit in ('kWh', 'MWh')
            elif category == 'water':
                unit = unit.lower()
                if unit == 'kwh':
                    assert 'vesi' in m['Description'].lower()
                    unit = 'm3'
                assert unit in ('m3', 'l')
            elif category == '':
                assert unit in (
                    'On/off', 'C', '%', 'Pa', 'Bar', 'μg/m3', 'ppb', '%RH',
                    'lx', 'kW', 'l/s', 'ppm', 'm3/s', 'kPa', 'M3', 'MWh',
                    'SFP', 'kohm', 'Status', 'bar', 'SFP-luku', 'm/s', 'Cd',
                )
            elif category == 'indoor conditions: temperature':
                assert unit == 'C'
            elif category == 'indoor conditions: relative humidity':
                assert unit == '%RH'
            elif category == 'indoor conditions: co2':
                assert unit == 'ppm'
            elif category == 'districtcooling':
                assert unit == 'kWh'
            elif category == 'pressure difference indoor / outdoor (pa)':
                assert unit == 'Pa'
            elif category.startswith('pm'):
                assert unit == 'μg/m3'
            elif 'tvoc' in category:
                assert unit == 'ppb'
            else:
                raise Exception('Unexpected unit: %s' % m)
        dpid = m['DataPointID']
        d = dict(
            id=dpid,
            category=m['Category'],
            name=m['Description'],
            unit=unit,
            identifier=m['Name']
        )
        out.append(d)

    return out


def camel_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def get_buildings():
    data = api_get('GetUserBuildings')
    from pprint import pprint
    pprint(data)
    out = []
    for b in data:
        o = {camel_to_snake(key): val for key, val in b.items()}
        o['id'] = o.pop('building_structure_id')
        o['type_name'] = o.pop('building_type')
        o['type_id'] = o.pop('building_type_id')
        construction_year = o.pop('construction_year')
        o['construction_year'] = int(construction_year.split('-')[0]) if construction_year else None
        try:
            lon = float(o.pop('latitude'))
            lat = float(o.pop('longitude'))
        except (ValueError, TypeError):
            o['geometry'] = None
        else:
            o['geometry'] = dict(type='Point', coordinates=[lon, lat])
        out.append(o)

    return out


def _determine_data_start_old(building_id, data_point_id):
    from utils.data_import import find_data_start

    def check_date(date):
        print(building_id, data_point_id)
        resp = get_daily_measurement_data(
            building_id, [data_point_id], date, date + timedelta(days=1)
        )
        if len(resp) < 1 or len(resp.time.unique()) <= 1:
            return False
        else:
            return True

    guesses = (date(2016, 1, 1), date(2017, 1, 1), date(2019, 5, 26))
    for guess in guesses:
        if check_date(guess):
            prev_day = guess - timedelta(days=1)
            if not check_date(prev_day):
                return guess

    should_be_valid = date.today() - timedelta(days=2)
    if not check_date(should_be_valid):
        return None

    ds = find_data_start(
        check_date, max_date=date.today() - timedelta(days=2),
        min_date=date(2015, 1, 1)
    )

    return ds


def _determine_data_start(building_id, data_point_id):
    for year in (2016, 2017, 2018, 2019):
        start = date(year, 1, 1)
        end = date(year, 12, 31)
        resp = get_daily_measurement_data(
            building_id, [data_point_id], start, end
        )
        if len(resp) == 0:
            continue
        if len(resp.value.unique()) <= 1:
            continue
        break
    if not len(resp):
        return None
    return resp.time.min().date()


def load_buildings():
    with open('data/nuuka/buildings.json', 'r') as f:
        data = json.load(f)

    return data


def _sensor_ok(sensor):
    if not sensor.get('data_start_date'):
        return False
    if not sensor['category']:
        return False
    if sensor['category'] not in ('electricity', 'heating', 'water'):
        return False
    # If there's an underscore in the name, it's probably one of the sub-sensors
    if '_' in sensor['name']:
        return False

    if not re.match(r'[^\W_]+ \d+', sensor['name']):
        return False

    return True


def get_all_measurements_for_building(building):
    sensors = [s for s in building['sensors'] if _sensor_ok(s)]
    if not sensors:
        return None

    min_date = date.fromisoformat(min([s['data_start_date'] for s in sensors]))
    last_date = date.today() - timedelta(days=1)
    start_date = min_date

    dfs = []

    while start_date < last_date:
        print(start_date)
        end_date = start_date + timedelta(days=30)
        sensor_ids = [s['id'] for s in sensors]
        df = get_measurement_data(building['id'], sensor_ids, start_date, end_date)
        start_date = end_date + timedelta(days=1)
        if len(df) < 5:
            continue
        dfs.append(df)

    if not dfs:
        open('data/nuuka/%s.nodata' % building['id'], 'a').close()
        return

    all_dfs = pd.concat(dfs, ignore_index=True)
    return all_dfs


def fetch_all():
    buildings = load_buildings()

    df = pd.DataFrame.from_records(buildings)
    df = df.drop(columns=['geometry', 'sensors']).set_index('id')
    df.to_parquet('data/nuuka/buildings.parquet')

    sensors = []
    for b in buildings:
        for s in b['sensors']:
            s = s.copy()
            s['building_id'] = b['id']
            sensors.append(s)
    df = pd.DataFrame.from_records(sensors).set_index('id')
    df.to_parquet('data/nuuka/sensors.parquet')

    for b in buildings:
        #if b['type_id'] not in (5, 6, 7, 47, 15):
        #    continue
        #if b['type_id'] not in (13, 17, 26, 27, 28, 29):
        #    continue
        if 'sensors' not in b:
            continue
        if os.path.exists('data/nuuka/%s.parquet' % b['id']) or os.path.exists('data/nuuka/%s.nodata' % b['id']):
            continue
        print('%s: %s' % (b['id'], b['description']))
        changed = False
        ok_sensors = [s for s in b['sensors'] if _sensor_ok(s)]
        for s in ok_sensors:
            print('\t%s (%s)' % (s['name'], s['category']))
            if not s['category'] or s['category'] not in ('electricity', 'heating', 'water'):
                continue
            data_start = s.get('data_start_date')
            if not s.get('data_start_date'):
                data_start = _determine_data_start(b['id'], s['id'])
                s['data_start_date'] = data_start.isoformat() if data_start else None
                changed = True
            if not data_start:
                continue
            print('\t\t%s' % s['data_start_date'])

        if changed:
            out = json.dumps(buildings, indent=4, ensure_ascii=False)
            with open('data/nuuka/buildings.json', 'w') as f:
                f.write(out)

        print('getting all measurements for %s' % b['id'])
        ret = get_all_measurements_for_building(b)
        if ret is None:
            open('data/nuuka/%s.nodata' % b['id'], 'a').close()
        else:
            ret.to_parquet('data/nuuka/%s.parquet' % b['id'], index=False)


if __name__ == '__main__':
    import settings

    set_api_params(base_url=settings.NUUKA_API_BASE_URL, api_key=settings.NUUKA_API_KEY)

    start_date = date.today() - timedelta(days=2)
    end_date = start_date + timedelta(days=1)

    fetch_all()
    exit()

    buildings = get_buildings()
    types = {b['type_id']: b['type_name'] for b in buildings}
    pprint(types)
    exit()

    # with open('data/nuuka/buildings.json', 'w') as f:
    #    json.dump(buildings, f, indent=4, ensure_ascii=False)

    for b in buildings:
        building_id = b['id']
        measurements = get_measurement_meta(building_id)
        data_point_ids = []
        print('%s: %s' % (b['id'], b['description']))
        b['sensors'] = measurements
        continue

        for m in measurements:
            print('  %d: %s (%s) %s' % (m['id'], m['name'], m['category'], m['identifier']))
            data_point_ids.append(m['id'])
            data_start = _determine_data_start(building_id, m['id'])
            print('\t%s' % data_start)

        if not data_point_ids:
            continue

        dpids = {m['id']: m for m in measurements}
        resp = get_measurement_data(building_id, data_point_ids, start_date, end_date)
        resp = sorted(resp, key=lambda x: x['data_point_id'])
        for r in resp:
            dp = dpids[r['data_point_id']]
            print('    %-15s %s %f' % (dp['description'], r['time'].isoformat(), r['value']))

    with open('data/nuuka/buildings.json', 'w') as f:
        json.dump(buildings, f, indent=4, ensure_ascii=False)
