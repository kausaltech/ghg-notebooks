import json
import requests
import numpy as np
from environs import Env


env = Env()
env.read_env()


class NumpyEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, np.ndarray):
            return obj.tolist()
        return json.JSONEncoder.default(self, obj)


def post_graph(fig, indicator_id):
    api_base = env.str('APLANS_API_BASE', None)
    if not api_base:
        return

    fig_json = fig.to_plotly_json()
    data = dict(
        data=fig_json,
        indicator='%s/indicator/%d/' % (api_base, indicator_id)
    )
    json_str = json.dumps(data, cls=NumpyEncoder)
    headers = {'Content-Type': 'application/json'}
    resp = requests.post('%s/indicator_graph/' % api_base, data=json_str, headers=headers)
    if resp.status_code != 201:
        print(resp.content)
    resp.raise_for_status()
    print("Graph posted successfully.")
