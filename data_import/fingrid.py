import io
from datetime import datetime, timezone, date
import logging

import requests
import pandas as pd
import numpy as np
import pint  # noqa
import pytz

from data_import.exceptions import NoRowsReturned


logger = logging.getLogger(__name__)


fingrid_api_key = None

LOCAL_TZ = pytz.timezone('Europe/Helsinki')


def fingrid_api_get(variable_id, quantity, unit, time_interval, start_time, end_time):
    assert fingrid_api_key is not None

    url = 'https://api.fingrid.fi/v1/variable/%s/events/csv' % variable_id

    if start_time.tzinfo is None:
        start_time = LOCAL_TZ.localize(start_time)
    if end_time.tzinfo is None:
        end_time = LOCAL_TZ.localize(end_time)

    start_time = start_time.replace(microsecond=0).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    end_time = end_time.replace(microsecond=0).astimezone(timezone.utc).isoformat().replace('+00:00', 'Z')
    params = {
        'start_time': start_time,
        'end_time': end_time,
    }
    headers = {
        'x-api-key': fingrid_api_key,
    }
    resp = requests.get(url, params=params, headers=headers)
    resp.raise_for_status()
    data = io.StringIO(resp.content.decode('utf-8'))
    df = pd.read_csv(data, header=0, parse_dates=['start_time', 'end_time'])

    if len(df) == 0:
        raise NoRowsReturned()

    # Make sure end_time - start_time have all even intervals
    delta = (df['end_time'] - df['start_time']).unique()
    assert len(delta) == 1
    delta = delta[0]
    delta = delta.astype('timedelta64[m]')
    # With a time interval of 3m, start_time == end_time
    assert time_interval in (3, 60)
    if time_interval == 3:
        assert delta == np.timedelta64(0, 'm')
    else:
        assert delta == np.timedelta64(time_interval, 'm')

    df.drop('end_time', axis=1, inplace=True)
    df.set_index(keys=['start_time'], inplace=True)
    df.index.rename('time', inplace=True)

    # Convert the UTC timestamps into the local timezone
    df.index = df.index.tz_convert('Europe/Helsinki')
    # Rename the 'value' column to the proper quantity
    df.rename(columns=dict(value=quantity), inplace=True)

    return df


def set_api_key(api_key):
    global fingrid_api_key

    fingrid_api_key = api_key


HOURLY = 3600
THREE_MIN = 3 * 60


MEASUREMENTS = {
    "electricity_production_hourly": {
        "unit": "MWh",
        "quantity": "energy",
        "start_date": date(2004, 1, 1),
        "variable_id": 74,
        "interval": HOURLY,
    },
    "electricity_production_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 192,
        "interval": THREE_MIN,
        "max_value": 50000,
    },
    "electricity_consumption_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 193,
        "interval": THREE_MIN,
        "max_value": 50000,
    },
    "electricity_consumption_hourly": {
        "unit": "MWh",
        "quantity": "energy",
        "start_date": date(2004, 1, 1),
        "variable_id": 124,
        "interval": HOURLY,
    },
    "electricity_net_export_russia_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 195,
        "interval": THREE_MIN,
    },
    "electricity_net_export_se1_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 10, 4),
        "variable_id": 87,
        "interval": THREE_MIN,
    },
    "electricity_net_export_estlink_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2008, 10, 25),
        "variable_id": 180,
        "interval": THREE_MIN,
    },
    "electricity_net_export_se3_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 10, 4),
        "variable_id": 89,
        "interval": THREE_MIN,
    },
    "electricity_net_export_no_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2008, 10, 25),
        "variable_id": 187,
        "interval": THREE_MIN,
    },
    "electricity_net_export_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 194,
        "interval": THREE_MIN,
    },
    "wind_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2012, 11, 27),
        "variable_id": 181,
        "interval": THREE_MIN,
    },
    "hydroelectric_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 191,
        "interval": THREE_MIN,
    },
    "nuclear_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 188,
        "interval": THREE_MIN,
    },
    "chp_electricity_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 201,
        "interval": THREE_MIN,
    },
    "industrial_chp_electricity_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 202,
        "interval": THREE_MIN,
    },
    "other_electricity_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "variable_id": 205,
        "interval": THREE_MIN,
    },
    "temperature_in_helsinki_3m": {
        "unit": "degC",
        "quantity": "temperature",
        "start_date": date(2010, 11, 8),
        "variable_id": 178,
        "interval": THREE_MIN,
    },
    "solar_power_generation_estimate": {
        "unit": "MWh",
        "quantity": "energy",
        "start_date": date(2017, 2, 24),
        "variable_id": 248,
        "interval": HOURLY,
    },
    "condensing_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2010, 11, 8),
        "end_date": date(2017, 9, 14),
        "variable_id": 189,
        "interval": THREE_MIN,  # 3 minutes
    },
    "purchase_price_of_production_imbalance_power": {
        "unit": "EUR/MWh",
        "quantity": "price",
        "variable_id": 92,
        "interval": HOURLY,
        "start_date": date(2009, 1, 1),
    }
}


# Make sure variables are unique
def _check_variable_uniqueness():
    variables = [x['variable_id'] for x in MEASUREMENTS.values()]
    if len(variables) != len(set(variables)):
        raise Exception("Duplicate variable ids: %s" % sorted(variables))


def get_measurements(measurement_name, start_time, end_time, include_units=False):
    m = MEASUREMENTS[measurement_name]
    df = fingrid_api_get(
        m['variable_id'], m['quantity'], m['unit'], int(m['interval'] / 60), start_time, end_time
    )
    # Filter buggy data if we know already that the values can't be more than 'max_value'
    max_value = m.get('max_value')
    if max_value:
        col = df[m['quantity']]
        buggy_count = len(df[col >= max_value])
        if buggy_count:
            logger.warning('Filtering %d buggy rows from %s' % (buggy_count, measurement_name))
            df = df[col < max_value]

    if include_units:
        # Add information about the units
        df[m['quantity']] = df[m['quantity']].astype('pint[%s]' % m['unit'])

    return df


def get_measurement_meta_data(measurement_name):
    return MEASUREMENTS[measurement_name]


_check_variable_uniqueness()


if __name__ == '__main__':
    from utils.data_import import find_data_start
    import settings

    set_api_key(settings.FINGRID_API_KEY)
    for name, m in [x for x in MEASUREMENTS.items() if not x[1].get('start_date')]:
        def check_date(check_date):
            start_time = datetime.combine(check_date, datetime.min.time())
            end_time = datetime.combine(check_date, datetime.max.time())
            try:
                get_measurements(name, start_time, end_time)
            except NoRowsReturned:
                return False

            return True

        ds = find_data_start(check_date)
        print(name)
        print('        "start_date": date(%d, %d, %d),' % (ds.year, ds.month, ds.day))
