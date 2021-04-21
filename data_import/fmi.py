import math
import re
import io
from datetime import datetime

import pandas as pd
import requests
from datetime import datetime, date, timedelta
import dateutil.parser
import pytz
from owslib.wfs import WebFeatureService
from lxml import etree

import settings


LOCAL_TZ = pytz.timezone('Europe/Helsinki')
URL_FORMAT = 'https://cdn.fmi.fi/weather-observations/products/heating-degree-days/lammitystarveluvut-{year}.utf8.csv'


def _get_heating_degree_days_excel():
    URL = 'https://cdn.fmi.fi/legacy-fmi-fi-content/documents/climate/hdd_1995-2007.xlsx'
    COLS = ('Kunta', 'I', 'II', 'III', 'IV', 'V', 'VI', 'VII', 'VIII', 'IX', 'X', 'XI', 'XII', 'Yhteensä')
    KUNTA_MAP = {
        "jomala": "Maarianhamina",
        "helsinki-vantaa": "Vantaa",
        "helsinki kaisaniemi": "Helsinki",
        "tampere-pirkkalan": "Tampere",
        "inari/ivalo": "Ivalo",
    }
    resp = requests.get(URL)
    resp.raise_for_status()
    f = io.BytesIO(resp.content)
    sheets = pd.read_excel(f, sheet_name=None, header=1, names=COLS)
    all_dfs = []
    for sheet_name, df in sheets.items():
        try:
            year = int(sheet_name)
        except ValueError:
            continue
        df = df.dropna().copy()
        df['Kunta'] = df.Kunta.map(lambda x: KUNTA_MAP.get(x.lower(), x.lower().capitalize()))
        df['Vuosi'] = year
        df = df.set_index('Vuosi').reset_index()
        all_dfs.append(df)
    return all_dfs


def get_heating_degree_days():
    dfs = _get_heating_degree_days_excel()
    for year in range(2008, 2018):
        url = URL_FORMAT.format(year=year)
        resp = requests.get(url)
        resp.raise_for_status()
        f = io.BytesIO(resp.content)
        df = pd.read_csv(f)
        df = df.rename(columns={df.columns[0]: 'Kunta'})
        df['Yhteensä'] = df['Vuosi']
        df['Vuosi'] = year
        df = df.set_index('Vuosi').reset_index()
        dfs.append(df)

    df = pd.concat(dfs, ignore_index=True, sort=False)
    return df


def get_meta(language, property_type):
    assert property_type in ('forecast', 'observation')
    assert language in ('fin', 'eng')
    url = 'https://opendata.fmi.fi/meta'
    params = {
        'observableProperty': property_type,
        'language': language
    }
    resp = requests.get(url, params=params)
    resp.raise_for_status()
    root = etree.fromstring(resp.content)
    properties = root.findall('.//{*}ObservableProperty')
    out = {}
    for prop in properties:
        d = {
            'id': prop.attrib['{http://www.opengis.net/gml/3.2}id'],
            'label': prop.find('{*}label').text,
            'base_phenomenon': prop.find('{*}basePhenomenon').text,
        }
        uom_el = prop.find('{*}uom')
        if uom_el is not None:
            d['unit'] = uom_el.attrib['uom']
        else:
            d['unit'] = None
        sm = prop.find('{*}StatisticalMeasure')
        if sm is not None:
            d['stat_function'] = sm.find('statisticalFunction')
            d['aggregation_time_period'] = sm.find('aggregationTimePeriod')

        assert d['id'] not in out
        out[d['id']] = d

    return out


def get_forecast(place=None, latlon=None, timestep=60, start_time=None, end_time=None):
    url = 'https://opendata.fmi.fi/wfs'
    wfs = WebFeatureService(url=url, version='2.0.0')
    params = {
        'timestep': timestep
    }
    if latlon:
        params['latlon'] = '%s,%s' % (latlon[0], latlon[1])
    elif place:
        params['place'] = place

    if start_time:
        params['starttime'] = start_time.isoformat().split('.')[0] + 'Z'
    query_id = 'fmi::forecast::harmonie::surface::point::multipointcoverage'

    resp = wfs.getfeature(storedQueryID=query_id, storedQueryParams=params)
    root = etree.fromstring(bytes(resp.read(), encoding='utf8'))
    print(str(etree.tostring(root), encoding='utf8'))

    result_time = root.find('.//{*}resultTime//{*}timePosition').text
    result_time = dateutil.parser.parse(result_time).astimezone(LOCAL_TZ)

    positions = root.find('.//{*}positions').text
    observations = root.find('.//{*}DataBlock/{*}doubleOrNilReasonTupleList').text
    fields = root.findall('.//{*}DataRecord/{*}field')
    field_names = [x.attrib['name'] for x in fields]

    positions = [re.findall(r'\S+', x.strip()) for x in positions.splitlines() if x.strip()]
    observations = [re.findall(r'\S+', x.strip()) for x in observations.splitlines() if x.strip()]

    data = []
    last_precipitation = None
    for pos, obs in zip(positions, observations):
        d = {field_name: float(sample) for field_name, sample in zip(field_names, obs)}
        ts = datetime.fromtimestamp(int(pos[2]))
        ts.replace(tzinfo=pytz.UTC)
        d['time'] = LOCAL_TZ.localize(ts)
        if 'PrecipitationAmount' in d:
            if last_precipitation:
                val = d['PrecipitationAmount']
                d['PrecipitationAmount'] -= last_precipitation
                last_precipitation = val
            else:
                last_precipitation = d['PrecipitationAmount']
        data.append(d)

    return dict(observations=data, meta=dict(result_time=result_time))


def convert_time(dt):
    if not isinstance(dt, datetime):
        dt = datetime.combine(dt, datetime.min.time())
        dt = LOCAL_TZ.localize(dt)
    return dt.astimezone(pytz.utc).isoformat().split('.')[0].split('+')[0] + 'Z'


def get_fmi_multipoint_data(*args, **kwargs):
    url = 'https://opendata.fmi.fi/wfs'
    wfs = WebFeatureService(url=url, version='2.0.0')
    params = {}
    timestep = kwargs.pop('timestep', None)
    if timestep:
        params['timestep'] = timestep

    latlon = kwargs.pop('latlon', None)
    if latlon:
        params['latlon'] = '%f,%f' % (latlon[0], latlon[1])
    place = kwargs.pop('place', None)
    if place:
        params['place'] = place
    bbox = kwargs.pop('bbox', None)
    if bbox:
        # min_lon, min_lat, max_lon, max_lat
        params['bbox'] = '%f,%f,%f,%f' % tuple(bbox)

    fmisid = kwargs.pop('fmi_sid', None)
    if fmisid:
        params['fmisid'] = fmisid

    # assert latlon or place or bbox

    start_time = kwargs.pop('start_time', None)
    if start_time:
        params['starttime'] = convert_time(start_time)

    end_time = kwargs.pop('end_time', None)
    if end_time:
        params['endtime'] = convert_time(end_time)

    max_locations = kwargs.pop('max_locations', None)
    if max_locations:
        params['maxlocations'] = max_locations

    if kwargs:
        raise Exception("Unknown kwargs: %s" % (', '.join(kwargs.keys())))

    query_id = args[0]
    if len(args) > 1:
        raise Exception("Unknown kwargs: %s" % (', '.join(args)))

    resp = wfs.getfeature(storedQueryID=query_id, storedQueryParams=params)
    data = resp.read()
    if isinstance(data, str):
        print(data)
        data = bytes(data, encoding='utf8')

    root = etree.fromstring(data)

    result_time = root.find('.//{*}resultTime//{*}timePosition').text
    result_time = dateutil.parser.parse(result_time).astimezone(LOCAL_TZ)

    positions = root.find('.//{*}positions').text
    observations = root.find('.//{*}DataBlock/{*}doubleOrNilReasonTupleList').text
    fields = root.findall('.//{*}DataRecord/{*}field')
    field_names = [x.attrib['name'] for x in fields]

    points = root.findall('.//{*}MultiPoint//{*}Point')
    point_by_coords = {}
    for el in points:
        pos = el.find('{*}pos').text.strip()
        name = el.find('{*}name').text
        point_by_coords[pos] = dict(name=name, coords=[float(x) for x in pos.split()])

    positions = [re.findall(r'\S+', x.strip()) for x in positions.splitlines() if x.strip()]
    observations = [re.findall(r'\S+', x.strip()) for x in observations.splitlines() if x.strip()]

    data = []
    for pos, obs in zip(positions, observations):
        d = {field_name: float(sample) for field_name, sample in zip(field_names, obs)}
        for key, val in d.items():
            if math.isnan(val):
                d[key] = None
        ts = datetime.fromtimestamp(int(pos[2]))
        d['time'] = LOCAL_TZ.localize(ts)
        d['location'] = point_by_coords['%s %s' % (pos[0], pos[1])]
        data.append(d)

    return dict(observations=data, meta=dict(result_time=result_time))


def validate_times(params, min_time):
    if isinstance(min_time, date):
        min_time = datetime.combine(min_time, datetime.min.time())

    start_time = params.get('start_time', None)
    if start_time:
        if isinstance(start_time, date):
            start_time = datetime.combine(start_time, datetime.min.time())
        if start_time < min_time:
            raise Exception("start_time %s smaller than min_time %s" % (start_time.isoformat(), min_time.isoformat()))


def get_air_quality_observations(*args, **kwargs):
    query_id = 'fmi::observations::airquality::hourly::multipointcoverage'
    return get_fmi_multipoint_data(query_id, **kwargs)


def get_sun_radiation_observations(*args, **kwargs):
    validate_times(kwargs, date(2012, 1, 1))
    query_id = 'fmi::observations::radiation::multipointcoverage'
    return get_fmi_multipoint_data(query_id, **kwargs)


def get_weather_observations(*args, **kwargs):
    #validate_times(kwargs, date())
    query_id = 'fmi::observations::weather::multipointcoverage'
    return get_fmi_multipoint_data(query_id, **kwargs)

# def get_observations(place=None, latlon=None, timestep=60, start_time=None, end_time=None):


def camel_to_under(name):
    s1 = re.sub('(.)([A-Z][a-z]+)', r'\1_\2', name)
    return re.sub('([a-z0-9])([A-Z])', r'\1_\2', s1).lower()


def read_observations(obs_type):
    props = get_meta('eng', 'observation')

    dfs = []

    try:
        all_obs = pd.read_parquet('%s-kumpula.parquet' % obs_type)
    except (FileNotFoundError, OSError):
        all_obs = None

    if all_obs is None:
        start_date = date(2016, 1, 1)
    else:
        start_date = all_obs['time'].max().date() - timedelta(days=1)

    while start_date < date.today():
        end_date = start_date + timedelta(days=6)
        print(start_date)
        if obs_type == 'radiation':
            func = get_sun_radiation_observations
        elif obs_type == 'weather':
            func = get_weather_observations
        else:
            raise Exception('invalid observation type: %s' % obs_type)
        data = func(
            fmi_sid=101004, timestep=60, start_time=start_date,
            end_time=end_date, max_locations=1
        )
        obs = data['observations']
        obs_out = []
        for o in obs:
            o['location'] = o['location']['name']
            for key, val in list(o.items()):
                prop = props.get(key.lower())
                if prop:
                    o[prop['label']] = val
                    del o[key]

            obs_out.append(o)

        start_date = end_date
        df = pd.DataFrame.from_records(obs_out)
        dfs.append(df)
        if len(dfs) == 10 or start_date >= date.today() - timedelta(days=2):
            sum_df = pd.concat(dfs, ignore_index=True)
            if all_obs is not None:
                all_obs = pd.concat([all_obs, sum_df], ignore_index=True, sort=False)
            else:
                all_obs = sum_df
            all_obs = all_obs.set_index('time')[~all_obs.index.duplicated(keep='first')].reset_index()
            all_obs.to_parquet('%s-kumpula.parquet' % obs_type, compression=None)
            dfs = []

    def printable_time(dt):
        return dt.strftime('%d.%m.%Y %H:%M')

    for obs in data['observations']:
        time = printable_time(obs.pop('time'))
        loc = obs.pop('location')
        if 'name' not in loc:
            break

        #labeled = [(props[key], val) for key, val in obs.items()]

        all_props = ', '.join(['%s: %s' % (prop_name, val) for prop_name, val in obs.items() if val is not None])
        print('%s %s: %s' % (time, loc, all_props))
        continue
        for prop_name, val in obs.items():
            prop = props.get(prop_name)

            if not prop:
                print('\t%s: %s' % (prop_name, val))
            else:
                print('\t%s: %s %s' % (prop['label'], val, prop['unit'] or ''))

    """
    data = get_forecast(place='Kumpula', timestep=10)

    for d in data['observations']:
        print('%s: lämpötila %4.1f°C, pilvisyys %3.0f %%, sade %.2f, tuuli %.1f m/s, tuulen suunta %s' % (
            str(d['time']), d['Temperature'], d['TotalCloudCover'], d['PrecipitationAmount'],
            d['WindSpeedMS'], deg_to_arrow(d['WindDirection'])
        ))

    print("Ennuste laskettu %s" % str(data['meta']['result_time']))

    #send_to_influxdb(data['observations'], 'Kumpula')
    """

if __name__ == '__main__':
    read_observations('weather')
    exit()


    import quilt

    USER = 'jyrjola'
    PACKAGE_BASE = 'fmi'

    def build_and_push(package, df):
        quilt.build('%s/%s/%s' % (USER, PACKAGE_BASE, package), df)
        quilt.push('%s/%s/%s' % (USER, PACKAGE_BASE, package), is_public=True)

    df = get_heating_degree_days()
    build_and_push('heating_degree_days', df)
