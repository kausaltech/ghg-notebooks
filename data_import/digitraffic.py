import csv
import re

from datetime import datetime, timedelta
import requests
import pytz
import pandas as pd


API_BASE = 'http://tie.digitraffic.fi/api/v1/'
RAW_BASE = 'https://aineistot.vayla.fi/lam/rawdata/'


def camel_str_to_snake(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def camel_dict_to_snake(d):
    for key, val in list(d.items()):
        snake_key = camel_str_to_snake(key)
        if snake_key != key:
            d[snake_key] = val
            del d[key]
        if isinstance(val, dict):
            camel_dict_to_snake(val)


def get_tms_stations():
    resp = requests.get(API_BASE + 'metadata/locations')
    resp = requests.get(API_BASE + 'metadata/tms-stations')
    resp.raise_for_status()
    data = resp.json()
    out = []
    for feat in data['features']:
        props = feat['properties']
        camel_dict_to_snake(props)
        props['id'] = props.pop('tms_number')
        assert feat['geometry']['type'] == 'Point'
        coords = feat['geometry']['coordinates']
        props['coordinates'] = coords[0:2]  # drop the height coordinate
        out.append(props)

    return out


RAW_DATA_FIELDS = [
    'station_id',
    'year',
    'day_of_year',
    'hour',
    'minute',
    'second',
    'second_10ms',
    'length',
    'lane',
    'direction',
    'vehicle_class',
    'speed',
    'faulty',
    'total_time',
    'time_interval',
    'queue_start'
]


VEHICLE_CLASSES = {
    1: 'ha/pa',  # (car or delivery van)
    2: 'kaip',   # (truck, no trailer)
    3: 'bus',
    4: 'kapp',   # (semi-trailer truck)
    5: 'katp',   # (truck with trailer)
    6: 'ha+pk',  # (car or delivery van with trailer)
    7: 'ha+av',  # (car or delivery van with trailer or with a mobile home)
}


LOCAL_TZ = pytz.timezone('Europe/Helsinki')


def fetch_tms_station_raw_data(ely_id, lam_id, measurement_date):
    day_of_year = measurement_date.timetuple().tm_yday
    year = measurement_date.year
    url = RAW_BASE + '%d/%s/lamraw_%s_%d_%d.csv' % (
        year, ely_id, lam_id, year % 1000, day_of_year
    )
    resp = requests.get(url)
    resp.raise_for_status()
    return resp.content.decode('utf8')


def parse_tms_station_raw_data(lam_id, measurement_date, content):
    lines = content.split('\n')
    reader = csv.DictReader(lines, fieldnames=RAW_DATA_FIELDS, delimiter=';')
    faulty = 0
    out = []
    lam_id = str(lam_id)
    for row in reader:
        # Skip faulty readings here
        if row.pop('faulty') == '1':
            faulty += 1
            continue

        for key, value in row.items():
            if key == 'station_id':
                assert value == lam_id
            elif key in ('length',):
                value = float(value)
            else:
                value = int(value)
            if key == 'vehicle_class':
                value = VEHICLE_CLASSES[value]
            row[key] = value

        year = row.pop('year')
        # Y2k ♥ -- this will break in 2070!!
        if year > 70:
            year += 1900
        else:
            year += 2000

        # Convert day of year to date
        dt = datetime(year, 1, 1) + timedelta(days=row.pop('day_of_year') - 1)
        dt = dt.replace(
            hour=row.pop('hour'), minute=row.pop('minute'), second=row.pop('second'),
            microsecond=row.pop('second_10ms') * 10 * 1000
        )

        assert dt.date() == measurement_date  # sanity check
        row['time'] = LOCAL_TZ.localize(dt)
        # Remove the "technical" fields
        del row['time_interval']
        del row['queue_start']
        del row['total_time']
        out.append(row)

    # If there are more than 5% faulty measurements, bail out
    if faulty > int(0.05 * len(out)):
        raise Exception("Too many faulty measurements (%d faulty, %d OK)" % (faulty, len(out)))

    COLUMN_ORDER = ['time', 'station_id', 'direction', 'lane', 'vehicle_class', 'length', 'speed']
    df = pd.DataFrame.from_records(out, index='time', columns=COLUMN_ORDER)
    return df


def get_tms_station_raw_data(ely_id, lam_id, measurement_date):
    content = fetch_tms_station_raw_data(ely_id, lam_id, measurement_date)
    return parse_tms_station_raw_data(lam_id, measurement_date, content)
