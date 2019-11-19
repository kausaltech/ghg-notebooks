import json
import datetime
import requests
import numpy as np
import pandas as pd

from environs import Env


env = Env()
env.read_env()


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


def _get_config():
    return env.str('APLANS_API_BASE', None), env.str('APLANS_API_KEY', None)


def _find_indicator(identifier):
    base_url, api_key = _get_config()

    resp = requests.get(url='%s/indicator/' % base_url, params={'filter[identifier]': identifier})
    resp.raise_for_status()
    data = resp.json()['data']
    if len(data) != 1:
        raise Exception('Indicator with identifier "%s" not found' % identifier)

    return data[0]['id']


def post_graph(fig, indicator_id):
    api_base, api_key = _get_config()
    if not api_base or not api_key:
        return

    fig_json = fig.to_plotly_json()
    data = dict(
        data=fig_json,
        indicator='%s/indicator/%d/' % (api_base, indicator_id)
    )
    json_str = json.dumps(data, cls=NumpyEncoder)
    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Token %s' % api_key
    }
    resp = requests.post('%s/indicator_graph/' % api_base, data=json_str, headers=headers)
    if resp.status_code != 201:
        err = resp.json()
        if 'errors' in err:
            print(err['errors'][0]['detail'])
        else:
            print(err)
    resp.raise_for_status()
    print("Graph posted successfully.")


def post_values(indicator_id, df_or_series):
    if isinstance(df_or_series, pd.Series):
        s = df_or_series
        df = pd.DataFrame(s.values, index=pd.to_datetime(s.index.astype('str')), columns=['value'])
        df.index.name = 'time'
        df = df.reset_index()
    else:
        df = df_or_series.copy()

    for col_name in df.columns:
        if col_name.lower() in ('vuosi', 'year'):
            df[col_name] = df[col_name].map(lambda x: '%s-12-31' % x)
            df['time'] = pd.to_datetime(df[col_name])
            df = df.drop(columns=col_name)
            break

    assert 'time' in df.columns

    if 'value' not in df.columns:
        assert len(df.columns) == 2
        cols = list(df.columns)
        cols.remove('time')
        df['value'] = cols[0]
        df = df.drop(columns=cols[0])

    df = df[['time', 'value']].copy()
    df.time = df.time.dt.date.map(lambda x: x.isoformat())
    data = df.to_dict('records')

    api_base, api_key = _get_config()
    if not api_base or not api_key:
        return

    indicator_id = _find_indicator(indicator_id)

    headers = {
        'Content-Type': 'application/json',
        'Authorization': 'Token %s' % api_key
    }
    resp = requests.post(
        '%s/indicator/%s/values/' % (api_base, indicator_id),
        json=dict(data=data), headers=headers
    )
    if resp.status_code != 200:
        print(resp.content)
    resp.raise_for_status()
    print("Values posted successfully.")


if __name__ == '__main__':
    import pandas as pd

    df = pd.DataFrame([
        {'time': '2019-01-01', 'value': 15},
        {'time': '2018-01-01', 'value': 12}
    ])
    df.time = pd.to_datetime(df.time)
    post_values('ghg_emissions_helsinki', df)
