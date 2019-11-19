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

# %%
DISTRICT_HEATING_OPERATOR = '005'  # Helen Oy
INPUT_DATASETS = [
    'jyrjola/energiateollisuus/district_heating_fuel',
    'jyrjola/energiateollisuus/district_heating_production',
    'jyrjola/statfi/fuel_classification',
    'jyrjola/ymparistotilastot/e12_helsingin_kaukolammon_sahkonkulutus',
    'jyrjola/ymparistotilastot/e21_kaup_rakennus_tyypeittain',
]

import pandas as pd
import pintpandas
from utils.quilt import load_datasets

import plotly
import plotly.graph_objs as go
import cufflinks as cf

import aplans_graphs

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)

dh_fuel_df, dh_production_df, fuel_classification, dh_demand_df, city_demand_df = load_datasets(INPUT_DATASETS)

# %%

# %%
fuel_co2 = fuel_classification[['code', 'co2e_emission_factor', 'is_bio']].set_index('code')
df = dh_fuel_df[dh_fuel_df.Operator == DISTRICT_HEATING_OPERATOR]
df = df.merge(fuel_co2, how='left', left_on='StatfiFuelCode', right_index=True)
df.co2e_emission_factor = df.co2e_emission_factor.astype('pint[t/TJ]')
df.Value = df.Value.astype('pint[GWh]')
operator_fuel_with_co2 = df.copy()
df['Emissions'] = (df.Value * df.co2e_emission_factor).pint.to('tonne').pint.m

df.loc[df.is_bio == True, 'Emissions'] = 0
emissions = df[df.Operator == DISTRICT_HEATING_OPERATOR].groupby('Year').Emissions.sum()
emissions.name = 'Emissions'

# %%
df = dh_production_df
df = df[df.Operator == DISTRICT_HEATING_OPERATOR]
#display(df)
heat_production = df[df.Quantity == 'Käyttö'].set_index('Year').Value
heat_production.name = 'Heat production'
electricity_production = df[df.Quantity == 'Kaukolämmön tuotantoon liittyvä sähkön nettotuotanto'].set_index('Year').Value
electricity_production.name = 'Electricity production'

# Determine the CHP alternate production energy consumptions according to the efficiency method
electricity_production_alternate = electricity_production / 0.39
heat_production_alternate = heat_production / 0.90
total = electricity_production_alternate + heat_production_alternate
heat_share = heat_production_alternate / total
heat_share.name = 'Share of heat production'

heat_production.iplot(title='Kaukolämmön kulutus Helsingissä')

# %%
df = pd.concat([emissions, heat_production, electricity_production, heat_share], axis=1)
df['HeatUnitEmissions'] = df.Emissions / df['Heat production'] * df['Share of heat production']
df['ElectricityUnitEmissions'] = df.Emissions / df['Electricity production'] * (1 - df['Share of heat production'])
df

# %%
df = operator_fuel_with_co2.set_index('Year')
last_year = df.loc[df.index.max()]
last_year = last_year[~last_year.StatfiFuelCode.isna()]
all_fuels = last_year.Value.pint.m.sum()
last_year['Share'] = last_year.Value / all_fuels
last_year

target_year_shares = {
    'Puupelletit ja -briketit': 30,
    'Kivihiili ja antrasiitti': 30,
    
}

# %%
pdf = dh_production_df
pdf = pdf[pdf.Operator == DISTRICT_HEATING_OPERATOR].set_index('Year')
fdf = dh_fuel_df
fdf = fdf[fdf.Operator == DISTRICT_HEATING_OPERATOR].set_index('Year')
fuel_use = fdf[fdf.Quantity == 'Kaukolämmön ja yhteistuotantosähkön tuotantoon käytetyt polttoaineet yhteensä'].Value
production = pdf[pdf.Quantity == 'Käyttö'].Value

heat_pump_production = pdf[pdf.Quantity == 'Lämmön talteenotto tai lämpöpumpun tuotanto'].Value
production = production.subtract(heat_pump_production, fill_value=0)
(production / fuel_use).iplot(
    layout=dict(
        title='Kaukolämmöntuotannon tehokkuus (kaukolämmön tuotanto verkkoon / polttoaineiden käyttö)',
        yaxis=dict(rangemode='tozero')
    )
)

# %%
from quilt.data.jyrjola import ymparistotilastot

df = ymparistotilastot.e15_helen_kaukolampo_jaahdytys()
df = df[df['Energiayritys'] == 'Helen']
df = df[df['Muuttuja'] == 'Kaukolämpö ja höyry']
df.set_index('Vuosi').value.iplot(
    layout=dict(
        title='Helenin kaukolämmön myynti',
        yaxis=dict(rangemode='tozero')
    )
)


# %%
df = dh_demand_df
df = df[df.Energiamuoto == 'Kaukolämpö']
df = df[df.Sektori == 'Kulutus yhteensä (GWh)']
df = df[df.Kunta == 'Helsinki']
s = df.set_index('Vuosi').value

aplans_graphs.post_values('district_heating_demand_helsinki', s)

# %%
df = city_demand_df
df = df[df.Rakennustyyppi == 'Kaikki yhteensä']
df = df[df.Muuttuja == 'Lämpö (GWh)']
s = df.set_index('Vuosi').value

aplans_graphs.post_values('district_heating_demand_city_buildings_helsinki', s)

# %%
df = dh_demand_df
df = df[df.Energiamuoto == 'Kaukolämpö']
df = df[df.Sektori == 'Ominaiskulutus sääkorjattu (kWh/m3)']
df = df[df.Kunta == 'Helsinki']
s = df.set_index('Vuosi').value

aplans_graphs.post_values('building_energy_efficiency_helsinki', s)

# %%
df = city_demand_df
df = df[df.Rakennustyyppi == 'Kaikki yhteensä']
df = df[df.Muuttuja == 'Sääkorjattu lämpö (kWh/m2)']
s = df.set_index('Vuosi').value
s

aplans_graphs.post_values('city_buildings_energy_efficiency_helsinki', s)
