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
# [![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/juyrjola/ghg-helsinki/master?urlpath=lab/tree/electric_vehicles.ipynb)
#
# This notebook estimates the greenhouse gases emitted by passenger cars in Helsinki. The main source data for the model is the [LIPASTO](http://lipasto.vtt.fi/en/index.htm) calculation system developed by [VTT Technical Research Centre of Finland Ltd.](http://www.vttresearch.com/)
#
# Click Run -> Run All Cells to run the calculations.

# %%
import math
import re
import pandas as pd
import numpy as np
import scipy
try:
    from quilt.data.jyrjola import lipasto
except ImportError:
    import quilt
    quilt.install('jyrjola/lipasto')
    from quilt.data.jyrjola import lipasto

import plotly
import plotly.graph_objs as go
import cufflinks as cf
import aplans_graphs

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)

# %% [markdown]
# First we load the municipality-specific data from LIPASTO. We are mostly interested in the total mileage in Helsinki specified by the road type (_highways_ or _urban driving_). The mileage column below is in million kilometres (_Mkm_) and the gases are in metric tonnes (_t_).

# %%
muni = lipasto.emissions_by_municipality().set_index(['Municipality', 'Vehicle', 'Road'])
out = muni.copy().xs('Helsinki')
out['mileage'] /= 1000000
display(out.sort_values(by='CO2e', ascending=False))

# %%
out.reset_index(['Vehicle', 'Road']).iplot(
    title='GHG emissions of road traffic in Helsinki in 2017',
    kind='pie', labels='Vehicle', values='CO2e'
)

# %% [markdown]
# We also read from LIPASTO the estimated mileage share per engine type and emission class.
#
# **NOTE:** The mileage share is a national estimate and might not be accurate for Helsinki.

# %%
mileage_share_per_engine_type = lipasto.mileage_per_engine_type().set_index(['Vehicle', 'Engine'])
display(mileage_share_per_engine_type.style.format("{:.2%}"))

# %% [markdown]
# To be able to calculate the total GHG emissions from the total mileage and the mileage ratios by engine type, we need to have an estimate on the unit emission factors for each engine. This we also get from LIPASTO.

# %%
car_unit_emissions = lipasto.car_unit_emissions().set_index(['Engine', 'Road'])
car_unit_emissions.xs('gasoline').xs('Urban')[['Car year', 'CO2e']].iplot(
    kind='line', x='Car year',
    layout=dict(
        yaxis=dict(rangemode='tozero', title='g CO2e/km', tickformat=',.0f', fixedrange=True),
        xaxis=dict(title='Car model year', fixedrange=True),
        title='Gasoline engine unit emissions in urban driving'
    )
)

def compare_unit_emissions():
    df = car_unit_emissions.reset_index()
    df = estimate_bev_unit_emissions(df, 2018)
    df.set_index(['Engine', 'Road'], inplace=True)
    df = df.groupby(['Road', 'Engine', 'Class']).min()['CO2e'].unstack('Class')['EURO 6']
    df = df.rename(columns={'EURO 6': 'CO2e (g/km)'})
    display(df.xs('Urban'))
    df.xs('Urban').sort_values().iplot(kind='bar')


# %% [markdown]
# For electric vehicles, we assume that the energy consumption (_kWh/km_) stays the same over the years, but the GHG emissions from the production of electricity decreases by about 50% until 2035.

# %%
def generate_electricity_unit_emission_series():
    years = pd.period_range(start=2018, end=2035, freq='Y')
    co2e_by_year = pd.Series(index=years, dtype=float)
    co2e_by_year.loc['2018'] = 100  # g CO2e/kWh
    co2e_by_year.loc['2030'] = 70   # g CO2e/kWh
    co2e_by_year.loc['2035'] = 45   # g CO2e/kWh
    co2e_by_year.interpolate(inplace=True)
    return co2e_by_year


def estimate_bev_unit_emissions(unit_emissions, year):
    ENERGY_CONSUMPTION = (
        ('Highways', 0.2),   # kWh/km
        ('Urban', 0.17)
    )
    rows = []
    electricity_ghg = generate_electricity_unit_emission_series()
    for road_type, kwh in ENERGY_CONSUMPTION:
        rows.append({
            "Engine": 'electric',
            "Road": road_type,
            "Car year": 2018,
            "CO2e": kwh * electricity_ghg[str(year)],
            "Class": "EURO 6"
        })
    return unit_emissions.append(pd.DataFrame(rows), ignore_index=True, sort=True)



generate_electricity_unit_emission_series().iplot(
    kind='line',
    layout=dict(
        yaxis=dict(rangemode='tozero', title='g CO2e/kWh', tickformat=',.0f', fixedrange=True),
        xaxis=dict(title='Year', fixedrange=True),
        title='Electric energy production emission factors'
    ),
)


# %% [markdown]
# Then we start estimating. We make some more probably incorrect assumptions to help keep the complexity of the calculation model in check. First, we assume that the mileage remains the same over the years. Second, we assume that the share of mileage driven with electric cars follows the [Bass diffusion model](https://en.wikipedia.org/wiki/Bass_diffusion_model) and that mileage is transferred from the diesel and gasoline engine categories starting from the most polluting engine types first.

# %%
def calculate_co2e_per_engine_type(ratios, unit_emissions, mileage_factor):
    # yearly passenger car kms in Helsinki
    km_in_helsinki = muni.xs('Helsinki').xs('Cars')['mileage'] * mileage_factor
    # df = ratios of passenger cars on the roads by engine type
    df = ratios.xs('Cars')
    roads = ('Urban', 'Highways')
    df = pd.concat([df * km_in_helsinki[road] for road in roads], keys=roads, names=['Road'])
    out = df * unit_emissions

    out /= 1000000000  # convert to kt (CO2e)
    return out


def bass_diffuse(t, p, q):
    e1 = math.e ** (-(p + q) * t)
    res = ((p + q) ** 2) / p
    res *= e1 / ((1 + q / p * e1) ** 2)
    return res


def calculate_bev_share(m, start_share, n_years):
    bev_share = start_share
    for t in range(n_years + 1):
        bev_share *= 1 + bass_diffuse(t, 0.05, 0.38) * m
    return bev_share


def estimate_mileage_ratios(year, bev_target_share, target_year):
    START_YEAR = 2018

    # Assume BEV share is increasing according to the Bass diffusion model
    # and that increase in share comes equally out of petrol and diesel engines
    # starting from the most polluting engine classes.
    df = mileage_share_per_engine_type.drop(columns='Sum')
    bev = df.loc['Cars', 'electric']
    bev_share_start = bev_share = bev['EURO 6']

    def estimate_bass_m(m):
        return abs(bev_target_share - calculate_bev_share(m, bev_share_start, target_year - START_YEAR))
    m = scipy.optimize.minimize_scalar(estimate_bass_m).x

    bev_share = bev['EURO 6'] = calculate_bev_share(m, bev_share_start, year - START_YEAR)

    share_change = bev_share - bev_share_start
    sums = df.loc['Cars'].sum(axis='columns')
    diesel_share = sums['diesel'] / (sums['diesel'] + sums['gasoline'])
    share_left = dict(diesel=share_change * diesel_share)
    share_left['gasoline'] = share_change - share_left['diesel']
    
    for i in range(0, 6 + 1):
        key = 'EURO %d' % i
        for eng in ('diesel', 'gasoline'):
            if not share_left[eng]:
                continue

            val = df.loc['Cars', eng][key]
            decrease = min(val, share_left[eng])
            df.loc['Cars', eng][key] -= decrease
            share_left[eng] -= decrease

    return df

def estimate_unit_emissions(year):
    df = car_unit_emissions.reset_index()
    df = estimate_bev_unit_emissions(df, year)
    df.set_index(['Engine', 'Road'], inplace=True)
    df = df.groupby(['Road', 'Engine', 'Class']).mean()['CO2e'].unstack('Class')
    return df


def generate_yearly_series(bev_target_in_2035, mileage_change):
    yearly_dfs = []
    yearly_mileage_change = math.exp(math.log(1 + mileage_change) / (2036 - 2018))
    for year in range(2018, 2036):
        share = estimate_mileage_ratios(year, bev_target_in_2035, 2035)
        unit_emissions = estimate_unit_emissions(year)
        mc = yearly_mileage_change**(year - 2018)
        co2e = calculate_co2e_per_engine_type(share, unit_emissions, mc)

        share = share.sum(axis='columns').xs('Cars')
        co2e = co2e.sum(level='Engine').sum(axis='columns')
        out = pd.concat([share, co2e], axis=1, keys=['Share', 'CO2e'], sort=True)
        out.index.name = 'Engine'
        yearly_dfs.append((year, out))

    out = pd.concat([x[1] for x in yearly_dfs], axis=0, keys=[x[0] for x in yearly_dfs], names=['Year'])

    out = out.reindex(('gasoline', 'diesel', 'electric'), level='Engine')
    # display(yearly_series.style.format("{:.2%}").format({'CO2e': '{:.0f}'}))
    out = out.unstack()
    
    return out


# %%
import ipywidgets


def plot(yearly_series):
    df = yearly_series
    fig1 = df.drop('CO2e', axis=1)['Share'].iplot(kind='line', asFigure=True)
    fig2 = go.Bar(x=df.index, y=df['CO2e'].sum(axis='columns'), yaxis='y2', name='Car traffic CO₂e.', opacity=0.2)

    shapes = [{
        'type': 'line',
        'xref': 'x',
        'yref': 'y2',
        'x0': df.index[0] - 0.5,
        'y0': 79,
        'x1': df.index[-1] + 0.5,
        'y1': 79,
        'line': {
            'color': 'green',
            'width': 3,
        },
    }]

    layout = go.Layout(
        xaxis=dict(title='year'),
        yaxis1=dict(overlaying='y2', tickformat=',.0%', rangemode='nonnegative'),
        yaxis2=dict(side='right', tickformat=',.0f', hoverformat='.0f', rangemode='nonnegative', title='kt'),
        barmode='overlay',
        shapes=shapes,
        title="The effect on cars' GHG emissions in Helsinki when the share of electric cars increase"
    )

    fig = go.Figure(data=(fig2,) + fig1.data, layout=layout)

    plotly.offline.init_notebook_mode(connected=True)
    #print(fig.to_plotly_json())

    plotly.offline.iplot(fig)

plot_out = ipywidgets.Output(layout={'height': '450px'})
display(plot_out)

bev_share = 45
mileage_change = 45

def refresh_plot():
    yearly_series = generate_yearly_series(bev_share / 100, mileage_change / 100)
    plot_out.clear_output()
    with plot_out:
        plot(yearly_series)

ev_share_slider = ipywidgets.IntSlider(min=10, max=80, value=bev_share, description='%')

def handle_ev_slider_change(change):
    plot_out.clear_output()
    global bev_share
    bev_share = change.new
    with plot_out:
        refresh_plot()
ev_share_slider.observe(handle_ev_slider_change, names='value')


mileage_change_slider = ipywidgets.IntSlider(min=-80, max=80, value=mileage_change, description='%')

def handle_mileage_change_slider_change(change):
    plot_out.clear_output()
    global mileage_change
    mileage_change = change.new    
    with plot_out:
        refresh_plot()
mileage_change_slider.observe(handle_mileage_change_slider_change, names='value')

display(ipywidgets.VBox([ipywidgets.Label('Share of electric vehicles in 2035'), ev_share_slider]))
display(ipywidgets.VBox([ipywidgets.Label('Change in mileage'), mileage_change_slider]))
refresh_plot()


# %%
