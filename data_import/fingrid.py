import io
from datetime import datetime, timedelta, timezone, date
import logging

import requests
import pandas as pd
import numpy as np
import pint  # noqa


logger = logging.getLogger(__name__)


fingrid_api_key = None


def fingrid_api_get(variable_id, quantity, unit, time_interval, start_time, end_time):
    assert fingrid_api_key is not None

    url = 'https://api.fingrid.fi/v1/variable/%s/events/csv' % variable_id

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

    # Make sure end_time - start_time have all even intervals
    delta = (df['end_time'] - df['start_time']).unique()
    assert len(delta) == 1
    delta = delta[0]
    delta = delta.astype('timedelta64[m]')
    # With a time interval of 3s, start_time == end_time
    if time_interval == 3:
        assert delta == np.timedelta64(0, 'm')
    else:
        other = np.timedelta64(time_interval, 'm')
        assert delta == np.timedelta64(time_interval, 'm')

    df.drop('end_time', axis=1, inplace=True)
    df.set_index(keys=['start_time'], inplace=True)
    df.index.rename('time', inplace=True)

    # Convert the UTC timestamps into the local timezone
    df.index = df.index.tz_convert('Europe/Helsinki')
    # Add information about the units
    df.value = df.value.astype('pint[%s]' % unit)
    # Rename the 'value' column to the proper quantity
    df.rename(columns=dict(value=quantity), inplace=True)

    return df


def set_api_key(api_key):
    global fingrid_api_key

    fingrid_api_key = api_key


MEASUREMENTS = {
    "electricity_consumption_hourly": {
        "unit": "MWh",
        "quantity": "energy",
        "start_date": date(2004, 1, 1),
        "variable_id": 74,
        "interval": 3600,  # 1 hour
    },
    "electricity_consumption_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 193,
        "interval": 3 * 60,  # 3 minutes
        "max_value": 50000,
    },
    "electricity_trade_russia_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 195,
        "interval": 3 * 60,  # 3 minutes
    },
    "electricity_net_import": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 194,
        "interval": 3 * 60,  # 3 minutes
    },
    "wind_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 181,
        "interval": 3 * 60,  # 3 minutes
    },
    "hydroelectric_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 191,
        "interval": 3 * 60,  # 3 minutes
    },
    "condensing_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 189,
        "interval": 3 * 60,  # 3 minutes
    },
    "nuclear_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 188,
        "interval": 3 * 60,  # 3 minutes
    },
    "chp_power_generation_3m": {
        "unit": "MW",
        "quantity": "power",
        "start_date": date(2011, 1, 1),
        "variable_id": 201,
        "interval": 3 * 60,  # 3 minutes
    },
    "temperature_in_helsinki": {
        "unit": "C",
        "quantity": "temperature",
        "start_date": date(2011, 1, 1),
        "variable_id": 178,
        "interval": 3 * 60,  # 3 minutes
    },
}

# Make sure variables are unique
VARIABLES = [x['variable_id'] for x in MEASUREMENTS.values()]
if len(VARIABLES) != len(set(VARIABLES)):
    raise Exception("Duplicate variable ids")


def get_measurements(measurement_name, start_time, end_time):
    m = MEASUREMENTS[measurement_name]
    df = fingrid_api_get(
        m['variable_id'], m['quantity'], m['unit'], int(m['interval'] / 60), start_time, end_time
    )
    # Filter buggy data if we know already that the values can't be more than 'max_value'
    max_value = m.get('max_value')
    if max_value:
        col = df[m['quantity']]
        buggy_count = len(df[col.pint.m >= max_value])
        if buggy_count:
            logger.warning('Filtering %d buggy rows from %s' % (buggy_count, measurement_name))
            df = df[col.pint.m < max_value]
    return df


def get_measurement_meta_data(measurement_name):
    return MEASUREMENTS[measurement_name]
