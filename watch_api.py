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


class NotFoundError(Exception):
    pass


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
                raise NotFoundError('No result for %s' % path)
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
        if resp.status_code not in (200, 201):
            print(resp.status_code)
            print(resp.text)
            print(resp.json())
        resp.raise_for_status()
        return resp.json()

    def get_organization(self, name):
        return self.rest_get(
            'organization', params=dict(name=name), expect_one_result=True
        )

    def get_plan(self, identifier):
        return self.rest_get('plan', params=dict(identifier=identifier), expect_one_result=True)

    def get_indicator(self, id=None, identifier=None, organization=None, name=None):
        if id is not None:
            data = self.rest_get('indicator/%d' % id)
            return data

        params = {}
        if identifier:
            params['identifier'] = identifier
        if organization:
            params['organization'] = organization['id']
        if name:
            params['name'] = name

        try:
            ind = self.rest_get('indicator', params=params, expect_one_result=True)
        except NotFoundError:
            return None
        return ind

    def create_indicator(self, data):
        return self.rest_post('indicator', data=data)

    def post_indicator_values(self, indicator, df_or_series):
        if isinstance(df_or_series, pd.Series):
            s = df_or_series
            idx = s.index.astype(str)
            if '-' not in idx[0]:
                idx_name = 'year'
            else:
                idx_name = 'date'
                idx = pd.to_datetime(idx)
            df = pd.DataFrame(s.values, index=idx, columns=['value'])
            df.index.name = idx_name
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

        data = df.to_dict('records')
        for d in data:
            d['categories'] = []

        self.rest_post('indicator/%d/values' % indicator['id'], data=data)


if __name__ == '__main__':
    api = KausalWatchAPI()
    org = api.get_organization(name='Tampereen kaupunki')
    plan = api.get_plan(identifier='tampere-ilmasto')
    upstream = api.get_indicator(organization=org, name='Kasvihuonekaasupäästöt Tampereella')

    try:
        ind = api.get_indicator(organization=org, name='Liikenteen khk-päästöt')
    except NotFoundError:
        ind = api.create_indicator(dict(
            name='Liikenteen khk-päästöt',
            organization=org['id'],
            unit='kt (CO₂e.)/a',
            levels=[dict(plan=plan['id'], level='strategic')],
            related_effects=[dict(
                effect_type='part_of',
                effect_indicator=upstream['id'],
                confidence_level='high'
            )],
        ))

    import pandas as pd

    df = pd.DataFrame([
        {'date': '2019-12-31', 'value': 15},
        {'date': '2018-12-31', 'value': 12}
    ])
    df.date = pd.to_datetime(df.date)
    api.post_indicator_values(ind, df)
