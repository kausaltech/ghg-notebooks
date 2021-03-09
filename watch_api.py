import datetime
import json
import logging

import numpy as np
import pandas as pd
import requests
from environs import Env

env = Env()
env.read_env()


logger = logging.getLogger(__name__)


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        if isinstance(obj, (datetime.date, datetime.datetime)):
            return obj.isoformat()
        return json.JSONEncoder.default(self, obj)


class KausalWatchAPI:
    def __init__(self, base_url=None, api_key=None):
        self.base_url = base_url or env.str('WATCH_API_URL')
        self.api_key = api_key or env.str('WATCH_API_KEY')
        if not self.base_url:
            raise Exception('WATCH_API_URL not set')

    def rest_get(self, path, params=None, expect_one_result=False):
        resp = requests.get(url='%s/%s/' % (self.base_url, path), params=params)
        resp.raise_for_status()
        data = resp.json()['results']

        if expect_one_result:
            if len(data) > 1:
                raise Exception('Multiple results returned for %s' % path)
            if not len(data):
                raise Exception('No result for %s' % path)
            data = data[0]

        return data

    def rest_post(self, path, params=None, data=None):
        if not self.api_key:
            raise Exception('WATCH_API_KEY not set')

        headers = {
            'Content-Type': 'application/json',
            'Authorization': 'Token %s' % self.api_key
        }
        resp = requests.post(
            '%s/%s/' % (self.base_url, path),
            json=data, headers=headers
        )
        if resp.status_code != 200:
            print(resp.text)
            print(resp.json())
        resp.raise_for_status()

        resp = requests.get(url='%s/%s/' % (self.base_url, path), params=params)
        resp.raise_for_status()
        data = resp.json()['results']

        return data

    def get_organization(self, name):
        return self.rest_get(
            'organization', params=dict(name=name), expect_one_result=True
        )

    def get_plan(self, identifier):
        return self.rest_get('plan', params=dict(identifier=identifier), expect_one_result=True)

    def get_indicator(self, identifier=None, organization=None, name=None):
        try:
            ind_id = int(identifier)
        except ValueError:
            ind_id = None

        if ind_id is not None:
            data = self.rest_get('indicator/%d' % identifier)
            return data

        params = {}
        if identifier:
            params['identifier'] = identifier
        if organization:
            params['organization'] = organization['id']
        if name:
            params['name'] = name

        return self.rest_get('indicator', params=params, except_one_result=True)

    def create_indicator(self, data):
        self.rest_post('indicator', data=data)

    def post_indicator_values(self, indicator, df_or_series):
        if isinstance(df_or_series, pd.Series):
            s = df_or_series
            df = pd.DataFrame(s.values, index=pd.to_datetime(s.index.astype('str')), columns=['value'])
            df.index.name = 'date'
            df = df.reset_index()
        else:
            df = df_or_series.copy()

        for col_name in df.columns:
            if col_name.lower() in ('vuosi', 'year'):
                df[col_name] = df[col_name].map(lambda x: '%s-12-31' % x)
                df['date'] = pd.to_datetime(df[col_name])
                df = df.drop(columns=col_name)
                break

        assert 'date' in df.columns

        if 'value' not in df.columns:
            assert len(df.columns) == 2
            cols = list(df.columns)
            cols.remove('date')
            df['value'] = cols[0]
            df = df.drop(columns=cols[0])

        df = df[['date', 'value']].copy()
        df.date = df.date.dt.date.map(lambda x: x.isoformat())

        print(df)
        yn = input('Press y/n: ')
        if yn.strip() != 'y':
            print('okay, no update')
            return

        data = df.to_dict('records')
        for d in data:
            d['categories'] = []

        print("Values posted successfully.")


if __name__ == '__main__':
    api = KausalWatchAPI()
    org = api.get_organization(name='Tampereen kaupunki')
    plan = api.get_plan(identifier='tampere-ilmasto')
    api.create_indicator(dict(
        name='Liikenteen khk-päästöt',
        organization=org['id'],
        unit='kt (CO₂e.)/a',
        levels=[dict(plan=plan['id'], level='strategic')]
    ))

    """
    import pandas as pd

    df = pd.DataFrame([
        {'time': '2019-01-01', 'value': 15},
        {'time': '2018-01-01', 'value': 12}
    ])
    df.time = pd.to_datetime(df.time)
    post_values('ghg_emissions_helsinki', df)
    """