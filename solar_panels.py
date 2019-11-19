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
from quilt.data.jyrjola import energiaatlas, hsy

orig_df = hsy.buildings()

# %%
df = orig_df[orig_df.kuntanimi == 'Helsinki']
#df = df[df.elec_kwh_v > 0]
df = df[df.kerrosala > 0]

# %%
df.kayt_luok.unique()
gdf = df.groupby('kayt_luok')[['panel_ala', 'elec_kwh_v', 'kerrosala']].sum()
gdf['kwh_per_ka'] = gdf['elec_kwh_v'] / gdf['kerrosala']
gdf['panel_ala_per_ka'] = gdf['panel_ala'] / gdf['kerrosala']
gdf['panel_ala_per_kwh'] = gdf['kwh_per_ka'] / gdf['panel_ala_per_ka']
gdf

# %%
df.kayt_luok.unique()
gdf = df.groupby('kayt_luok')[['panel_ala', 'elec_kwh_v', 'kerrosala']].sum()
gdf['kwh_per_ka'] = gdf['elec_kwh_v'] / gdf['kerrosala']
gdf['panel_ala_per_ka'] = gdf['panel_ala'] / gdf['kerrosala']
gdf['panel_ala_per_kwh'] = gdf['kwh_per_ka'] / gdf['panel_ala_per_ka']
gdf
