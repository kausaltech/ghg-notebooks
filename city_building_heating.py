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
# # GHG emission impact of change in heating efficiency of buildings
#
# [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/juyrjola/ghg-helsinki/master?urlpath=lab/tree/city_building_heating.ipynb)
#

# %%
INPUT_DATASETS = [
    'jyrjola/ymparistotilastot/e21_kaup_rakennus_tyypeittain', 'jyrjola/hsy/pks_khk_paastot',
    'jyrjola/aluesarjat/hginseutu_va_ve01_vaestoennuste_pks',
]

import pandas as pd
import scipy
import importlib

from utils import dict_merge
from utils.quilt import load_datasets
import aplans_graphs

import plotly
import plotly.graph_objs as go
import cufflinks as cf

kaup_rakennus_tyypeittain, pks_khk_paastot, vaestoennuste_pks = load_datasets(INPUT_DATASETS)

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)

# %%
df = kaup_rakennus_tyypeittain.copy()
df.Vuosi = df.Vuosi.astype(int)
buildings_by_type = df.pivot_table(index=['Vuosi', 'Rakennustyyppi'], columns='Muuttuja', values='value').reset_index()
display(buildings_by_type)
kaup_rakennus_tyypeittain = df[df.Rakennustyyppi == 'Kaikki yhteensä'].pivot(index='Vuosi', columns='Muuttuja', values='value')

# %%
df = vaestoennuste_pks
df = df[(df.Laadintavuosi == 'Laadittu 2018') & (df.Sukupuoli == 'Molemmat sukupuolet') & (df.Ikä == 'Väestö yhteensä')]
df = df[df.Alue == 'Helsinki']
df.Vuosi = df.Vuosi.astype(int)
pop_forecast = df.set_index('Vuosi')['value']


# %%
def generate_forecast_series(historical_series, year_until):
    s = historical_series
    start_year = s.index.min()
    res = scipy.stats.linregress(s.index, s)

    years = list(range(start_year, year_until + 1))
    predictions = pd.Series([res.intercept + res.slope * year for year in years], index=years)
    last_val = s[start_year]
    return predictions

def generate_scenarios(historical_series, goal_series=None, other_series=None, layout={}):
    s = historical_series.interpolate()
    forecast = generate_forecast_series(s, 2035)
    data = [
        go.Scatter(
            x=s.index, y=s, connectgaps=True, name='Mitattu',
            line=dict(color='grey'),
        ),
        go.Scatter(
            x=list(forecast.index), y=list(forecast), name='Nykytrendi', mode='lines',
            line=dict(color='blue', dash='dash'), opacity=0.5
        )
    ]
    if goal_series is not None:
        cs = s.combine_first(goal_series)
        cs = cs.reindex(range(cs.index.min(), 2035+1))
        cs = cs.interpolate(method='pchip')
        cs = cs.loc[s.index.max():]
        data.append(go.Scatter(
            x=cs.index, y=cs, name='Goal', mode='lines',
            line=dict(color='green', dash='dash')
        ))
        forecast = cs

    if other_series is not None:
        os = other_series
        os = os.loc[(os.index >= s.index.min()) & (os.index <= forecast.index.max())]
        data.append(go.Scatter(
            x=os.index, y=os, yaxis='y2'
        ))

    d = {
        "xaxis": dict(title='Vuosi', fixedrange=True),
        "yaxis": dict(fixedrange=True)
    }
    if other_series is not None:
        d['yaxis2'] = dict(
            title='Y2',
            fixedrange=True,
            overlaying='y',
            side='right'
        )

    dict_merge(d, layout)
    fig = go.Figure(data=data, layout=d)

    combined = pd.concat([s, forecast], axis='index')
    combined = combined[~combined.index.duplicated(keep='first')]
    
    return fig, combined


def draw_target_line(series, target_value, yref='y'):
    x_min, x_max = series.index.min(), series.index.max()
    shape = {
        'type': 'line',
        'xref': 'x',
        'yref': 'y',
        'x0': x_min - 0.5,
        'y0': target_value,
        'x1': x_max + 0.5,
        'y1': target_value,
        'line': {
            'color': 'green',
            'width': 3,
        },
    }
    return shape


fig, heat_use_series = generate_scenarios(kaup_rakennus_tyypeittain['Sääkorjattu lämpö (kWh/m2)'], layout={
    "yaxis": dict(title='Sääkorjattu ominaiskulutus (kWh/m²)', hoverformat='.0f'),
    "title": "Helsingin kaupungin omistamien kiinteistöjen ominaislämmönkulutus"
})

#aplans_graphs.post_graph(fig, 4)
plotly.offline.iplot(fig, config=dict(showLink=False))

# %%

# %%
fig, area_series = generate_scenarios(kaup_rakennus_tyypeittain['Pinta-ala (1000 m2)'], layout={
    "yaxis": dict(title='Pinta-ala (1000 m²)', hoverformat='.0f'),
    "title": "Helsingin kaupungin omistamien kiinteistöjen pinta-ala"
})
plotly.offline.iplot(fig, config=dict(showLink=False))

def calculate_emission_factor_for_heat(goal=True):
    df = pks_khk_paastot.copy()
    heat_rows = df.loc[(df['Kaupunki'] == 'Helsinki') & (df['Sektori2'] == 'Kaukolämpö')]
    by_year = heat_rows.groupby('Vuosi').sum().drop(1990)
    emission_factors = by_year['Päästöt'] / by_year['Energiankulutus'] * 1000

    goal = pd.Series([128.8, 49.1], index=[2030, 2035])

    layout = dict(
        title='Lämmöntuotannon päästökerroin',
        yaxis=dict(title='g CO2e / kWh', hoverformat='.1f')
    )
    fig, combined = generate_scenarios(emission_factors, goal_series=goal, layout=layout)
    #aplans_graphs.post_graph(fig, 9)
    plotly.offline.iplot(fig, config=dict(showLink=False))
    return combined



emission_factors = calculate_emission_factor_for_heat()
out = area_series * heat_use_series * emission_factors / 1000000
out = out.drop_duplicates()
#display(draw_target_line(out, 234*.20))
fig = go.Figure(
    data=[
        go.Bar(x=out.index, y=out, opacity=0.5),
    ],
    layout=dict(
        title='Helsingin omistamien kiinteistöjen lämmönkulutuksen khk-päästöt',
        shapes=[draw_target_line(out, 234*.20)],
        yaxis=dict(title='kt CO2e', hoverformat='.0f')
    ),
)

plotly.offline.iplot(fig, config=dict(showLink=False))

# %%

# %%
