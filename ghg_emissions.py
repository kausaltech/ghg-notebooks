# -*- coding: utf-8 -*-
# ---
# jupyter:
#   jupytext:
#     formats: ipynb,py:percent
#     text_representation:
#       extension: .py
#       format_name: percent
#       format_version: '1.3'
#       jupytext_version: 1.11.0
#   kernelspec:
#     display_name: Python 3
#     language: python
#     name: python3
# ---

# %% [markdown]
# # Greenhouse gas emissions prediction for the City of Helsinki
#
# This model uses the greenhouse gas (GHG) [estimates](https://hsy.fi/paastot) calculated by the
# Helsinki Region Environmental Services Authority HSY.
#
# The GHG emission target for the City of Helsinki is 80% reduction from
# the levels of the year 1990. Trend prediction is done using a simple
# linear regression model based on historical data.

# %%
INPUT_DATASETS = ['hsy/pks_khk_paastot']

import math
import re
import scipy
import scipy.stats
import pandas as pd
import numpy as np
import importlib
from utils.dvc import load_datasets

import plotly
import plotly.graph_objs as go
import cufflinks as cf

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)

hsy_ghg_emissions = load_datasets(INPUT_DATASETS)

# %%
df = hsy_ghg_emissions

# %%
df = df[df.Kaupunki == 'Helsinki']
df = df.groupby(['Vuosi', 'Sektori1'])['Päästöt'].sum()

# %%
# load_datasets('tampere/scenarios/bau')
df = pd.DataFrame([[1,2,3], [False] * 3], ['a', 'b', 'c'])


# %%

# %%

# %%
def generate_forecast_series(historical_series, year_until):
    s = historical_series
    start_year = s.index.min()
    res = scipy.stats.linregress(s.index, s)
    print(res)

    years = list(range(start_year, year_until + 1))
    predictions = pd.Series([res.intercept + res.slope * year for year in years], index=years)
    last_val = s[start_year]
    return predictions

def generate_plot(ghg_emissions, forecast, target, forecast_target_day):
    """Generate a plot with the historical GHG estimates and the forecast lines
    """
    df = ghg_emissions.reset_index().set_index('Vuosi')
    last_year = df.index.max()
    # Order the sectors based on impact in the last measured year
    sectors = list(df.xs(last_year).sort_values('Päästöt', ascending=False)['Sektori1'])
    data = []
    for sector_name in sectors:
        s = df[df['Sektori1'] == sector_name]['Päästöt']
        bar = go.Bar(x=s.index, y=s, name=sector_name, legendgroup='historical')
        data.append(bar)

    # There is discontinuity between 1990 and 2000 which messes up the plot,
    # so show only years from 2000 onwards.
    forecast = forecast[forecast.index >= 2000]
    forecast = forecast[forecast.index <= forecast_target_day.index[0].year + 1]
    
    current_trend_line = go.Scatter(
        x=forecast.index, y=forecast, name='Nykytrendi',
        line=dict(color='blue', dash='dash'), opacity=0.5,
        legendgroup='prediction'
    )
    data.append(current_trend_line)

    last_year_ghg = ghg_emissions.xs(last_year)['Päästöt'].sum()
    goal_series = pd.Series([last_year_ghg] + list(target), index=[last_year, target.index[0]])
    goal_series = goal_series.reindex(range(goal_series.index.min(), goal_series.index.max() + 1))
    goal_series.interpolate(inplace=True)
    #for key, val in goal_series.items():
    #    print('%d\t%f' % (key, val))

    goal_line = go.Scatter(
        x=goal_series.index, y=goal_series, name='Tavoite', mode='lines',
        line=dict(color='green', dash='dash'), opacity=0.5,
        legendgroup='prediction'
    )
    data.append(goal_line)

    shapes = [
        # The dashed line separating 1990 from 2000
        {
            'type': 'line',
            'x0': 0.5,
            'x1': 0.5,
            'xref': 'x',
            'y0': 0,
            'y1': 1,
            'yref': 'paper',
            'opacity': 0.8,
            'line': {
                'color': '#555',
                'width': 2,
                'dash': 'dash',
            }
        }, 
        # The shaded indicating the target area for emissions 
        {
            'type': 'rect',
            'xref': 'paper',
            'yref': 'y',
            'x0': 0,
            'y0': 0,
            'x1': 1,
            'y1': target.values[0],
            'line': {
                'width': 0,
            },
            'fillcolor': 'green',
            'opacity': 0.3
        },
    ]

    xaxis = {
        'type': 'category',
    }
    yaxis = {
        'hoverformat': '.3r',
        'separatethousands': True,
        'title': 'KHK-päästöt (kt CO₂-ekv.)'
    }

    layout = go.Layout(barmode='stack', xaxis=xaxis, yaxis=yaxis, shapes=shapes, separators=', ')
    fig = go.Figure(data=data, layout=layout)
    fig.iplot()
    #aplans_graphs.post_graph(fig, 5)


def estimate_ghg_emissions():
    df = hsy_ghg_emissions.copy()
    # We're examining the data about only Helsinki
    ghg_emissions = df[df['Kaupunki'] == 'Helsinki'].drop('Kaupunki', axis=1)
    # Sum all the sub-sectors
    ghg_emissions = ghg_emissions.groupby(['Vuosi', 'Sektori1']).sum()

    # Sum the all the emissions sectors by year
    sum_emissions = ghg_emissions.groupby('Vuosi')['Päästöt'].sum()
    
    # The GHG emissions target is 20 % of year 1990 emissions
    target = sum_emissions[1990] * .20
    left_to_go = sum_emissions.loc[sum_emissions.index.max()] - target
    display(left_to_go)

    latest_year = sum_emissions.index.max()
    sum_emissions = sum_emissions.loc[sum_emissions.index > latest_year - 20]
    display(sum_emissions)

    # Estimate GHG emissions based on a linear regression over the current
    # data.
    forecast = generate_forecast_series(sum_emissions, 2100)
    # Assume the yearly GHG emissions "land" on the last day of the year
    days = forecast.index.astype(str) + '-12-31'
    daily_forecast = forecast.copy()
    daily_forecast.index = pd.to_datetime(days, format='%Y-%m-%d')
    # Generate a daily emission series with linear interpolation
    daily_index = pd.date_range(daily_forecast.index.min(), daily_forecast.index.max())
    daily_forecast = daily_forecast.reindex(daily_index).interpolate()

    # The first day when we finally reach our target
    day_when_target_reached = daily_forecast[daily_forecast < target].head(1)
    target_day = pd.Series(target, index=[2035])
    generate_plot(ghg_emissions, forecast, target_day, day_when_target_reached)
    print(day_when_target_reached)

estimate_ghg_emissions()


# %%
df = hsy_ghg_emissions
df = df.query('Kaupunki == "Helsinki"').groupby(['Vuosi', 'Sektori1', 'Sektori2']).sum()
df = df.reset_index()
s = df.groupby('Vuosi').sum()['Päästöt']
aplans_graphs.post_values('ghg_emissions_helsinki', s)

main_df = df.groupby(['Vuosi', 'Sektori1'])['Päästöt'].sum().reset_index().pivot(values='Päästöt', index='Vuosi', columns='Sektori1')
aplans_graphs.post_values('ghg_emissions_electricity_helsinki', main_df['Sähkö'])
aplans_graphs.post_values('ghg_emissions_transport_helsinki', main_df['Liikenne'])
aplans_graphs.post_values('ghg_emissions_heating_helsinki', main_df['Lämmitys'])


# %%
s = df.query("Sektori2 == 'Kaukolämpö'").set_index('Vuosi')['Päästöt']
aplans_graphs.post_values('ghg_emissions_district_heating_helsinki', s)


# %%
df = hsy_ghg_emissions
df = df[df.Kaupunki == "Helsinki"]
#df = df[df.Vuosi == 2018]
#df = df.groupby(['Vuosi', 'Sektori1', 'Sektori2', 'Sektori3']).sum()

all_sectors = set(df.Sektori1.unique()) | set(df.Sektori2.unique()) | set(df.Sektori3.unique())
edges = set()
for row_id, row in df[df.Vuosi == 2018].iterrows():
    edges.add((row.Sektori1, 'Päästöt'))
    edges.add((row.Sektori2, row.Sektori1))
    if row.Sektori3 != row.Sektori2:
        edges.add((row.Sektori3, row.Sektori2))

dag = graphviz.Digraph()
for parent, child in edges:
    dag.edge(parent, child)

#df[df.Vuosi == 2015]
dag.edge('Kulutussähkön energiankulutus', 'Kulutussähkö')
dag.edge('Sähköntuotannon päästökerroin', 'Kulutussähkö')
dag.edge('Sähköntuotannon päästökerroin', 'Metrot')
dag.edge('Sähköntuotannon päästökerroin', 'Raitiovaunut')
dag.edge('Sähköntuotannon päästökerroin', 'Lähijunat')
dag
