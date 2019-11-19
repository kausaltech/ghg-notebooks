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
import pandas as pd
from datetime import date

pd.options.display.max_columns = None

# %%
atlas_data_df = pd.read_excel('data/data_atlas.xlsx', headers=0)

# %%
df = atlas_data_df
df.Valmistunut = pd.to_datetime(df.Valmistunut, format='%Y-%m-%d', errors='coerce')
df.loc[df.Valmistunut.isin(['2099-01-01', '2050-12-31']), 'Valmistunut'] = pd.NaT

CONSUMPTION_TYPES = (
    'Lämmitys yhteensä', 'Tilojen lämmitys', 'Veden lämmitys', 'Kiinteistösähkö', 'Käyttäjäsähkö'
)
BUILDING_CLASS_ENERGY_CONSUMPTION = (
    ('LPR4', '65', '57', '8', '27', '53'),
    ('LPR3', '138', '130', '8', '26', '53'),
    ('LPR2', '173', '165', '8', '16', '53'),
    ('LPR1', '291', '283', '8', '14', '53'),
    ('OKT4', '116', '93', '23', '12', '33'),
    ('OKT3', '176', '153', '23', '10', '33'),
    ('OKT2', '228', '205', '23', '8', '33'),
    ('OKT1', '286', '263', '23', '8', '33'),
    ('RKT9', '82', '45', '37', '16', '40'),
    ('RKT8', '112', '75', '37', '16', '40'),
    ('RKT7', '132', '95', '37', '15', '40'),
    ('RKT6', '135', '98', '37', '15', '40'),
    ('RKT5', '136', '99', '37', '14', '40'),
    ('RKT4', '140', '103', '37', '14', '40'),
    ('RKT3', '143', '106', '37', '13', '40'),
    ('RKT2', '147', '110', '37', '13', '40'),
    ('RKT1', '130', '93', '37', '12', '40'),
)
efficiency_by_class = {x[0]: {z[0]: int(z[1]) for z in zip(CONSUMPTION_TYPES, x[1:])} for x in BUILDING_CLASS_ENERGY_CONSUMPTION}


# %%
pd.options.display.max_columns = None
for col_name, energy_name in (('Lämmönkulutus', 'Lämmitys yhteensä'), ('Kiinteistösähkönkulutus', 'Kiinteistösähkö'), ('Käyttäjäsähkönkulutus', 'Käyttäjäsähkö')):
    energy_efficiency = df.rakennusryhma.map(lambda x: efficiency_by_class.get(x, {}).get(energy_name))
    df[col_name] = df.Kokonaisala * energy_efficiency

# %%
"""
import quilt
from quilt.data.jyrjola import energiaatlas

node = energiaatlas
node._set(['buildings'], df)
quilt.build('jyrjola/energiaatlas', node)
quilt.push('jyrjola/energiaatlas', is_public=True)
"""

# %%
from quilt.data.jyrjola import energiaatlas, hsy, karttahel
atlas_buildings = energiaatlas.buildings()
hsy_buildings = hsy.buildings()
hel_buildings = karttahel.buildings()
#atlas_buildings
#atlas_buildings.query("rakennusryhma == 'RKT1'")

# %%
hdf = hsy_buildings[['vtj_prt', 'elec_kwh_v']].dropna().set_index('vtj_prt')
df = atlas_buildings.dropna(subset=['VTJ_PRT']).set_index('VTJ_PRT')
atlashsy = df.merge(hdf, left_index=True, right_index=True)

# %%

# %%
all_heat = df['Lämmönkulutus'].sum()
#(df.groupby('rakennusryhma')['Lämmönkulutus', 'elec_kwh_v'].sum() / all_heat * 100).round(1).astype('str') + ' %'
grouped = df.groupby('rakennusryhma')['Lämmönkulutus', 'Kiinteistösähkönkulutus'].sum() / 1000000
#df = grouped.rename(columns=dict({'Lämmönkulutus': 'Lämmönkulutus (GWh)', 'elec_kwh_v': 'PV-potentiaali (GWh)', 'Kiinteistösähkönkulutus': 'Kiinteistösähkönkulutus (GWh)'})).round(1)
grouped

# %%
df = atlas_buildings
df['rakennusryhma'] = df.rakennusryhma.fillna('ZZZ')
df.groupby('kayttotarkoitus').Kokonaisala.sum().sort_values(ascending=False)
#df.groupby('rakennusryhma').count()
#df[df.rakennusryhma.str.startswith('RKT')].groupby('kayttotarkoitus').Kokonaisala.sum()
df['Year'] = df.Valmistunut.dt.year
(df.groupby('Year')['Lämmönkulutus'].sum() / 1000000).cumsum()

# %%
hsy_buildings.query("kunta == '091'").elec_kwh_v.sum()/1000/1000

# %%
