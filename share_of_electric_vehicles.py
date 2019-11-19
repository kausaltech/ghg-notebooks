# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.2'
#       jupytext_version: 1.2.4
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Greenhouse gas emissions of passenger car traffic in Helsinki
#
# [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/juyrjola/ghg-helsinki/master?urlpath=lab/tree/share_of_electric_vehicles.ipynb)
#
# Click Run -> Run All Cells to run the calculations.

# %%
INPUT_DATASETS = ['jyrjola/ymparistotilastot']

import math
import re
import pandas as pd
import numpy as np
import importlib

for dataset in INPUT_DATASETS:
    mod_path = dataset.replace('/', '.')
    try:
        mod = importlib.import_module('quilt.data.%s' % mod_path)
    except ImportError:
        import quilt
        quilt.install(dataset)

from quilt.data.jyrjola import ymparistotilastot
from utils import dict_merge
import aplans_graphs

import plotly
import plotly.graph_objs as go
import cufflinks as cf

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)

# %%
df = ymparistotilastot.l34_polttoaine_tavoitteet().copy()
display(df.set_index(['Vuosi']))

df['Muut'] = df['Maakaasu'] + df['Bensiini/maakaasu'] + df['Bensiini/etanoli'] + df['Bensiini/sähkö'] + df['Sähkö']+ df['Diesel/sähkö'] + df['Muu/tuntemato']
for name in ['Bensiini', 'Diesel', 'Muut']:
    df[name] = df[name] / df['Yhteensä']

start_year = df['Vuosi'].max()
start_val = df.loc[df['Vuosi'] == start_year]['Muut'].sum()
target_series = pd.Series([start_val, 0.20], index=[start_year, 2020])
target_series = target_series.reindex(range(target_series.index.min(), target_series.index.max() + 1))
target_series = target_series.interpolate(method='pchip')


data = [
    go.Scatter(
        x=df['Vuosi'], y=df['Bensiini'], name='Bensiini',
        line=dict(color='#ffc61e'),
    ),
    go.Scatter(
        x=df['Vuosi'], y=df['Diesel'], name='Diesel',
        line=dict(color='#c2a251'),
    ),
    go.Scatter(
        x=df['Vuosi'], y=df['Muut'], name='Muut',
        line=dict(color='#00d7a7'),
    ),
    go.Scatter(
        x=target_series.index, y=target_series, name='Tavoite',
        line=dict(color='#009246', dash='dash')
    )
]
d = {
    "xaxis": dict(title='Vuosi', fixedrange=True),
    "yaxis": dict(fixedrange=True, tickformat=',.0%', rangemode="tozero"),
    "title": "Helsingissä ensirekisteröidyjen henkilöautojen osuudet käyttövoiman mukaan",
}
fig = go.Figure(data=data, layout=d)

plotly.offline.iplot(fig)
aplans_graphs.post_graph(fig, 14)


# %%
