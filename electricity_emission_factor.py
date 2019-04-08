# -*- coding: utf-8 -*-
# # Electricity production GHG emissions factor for Finland
#
# In Finland, the greenhouse gas emissions from electricity production are mostly associated with burning fuels in power plants. In this analysis, we calculate the emissions factor to supply the Finnish national electricity grid with one unit of energy. 

# +
INPUT_DATASETS = ['jyrjola/fingrid_hourly/power']

from datetime import timedelta
import pandas as pd
import numpy as np
import altair as alt
from utils.quilt import load_datasets
from data_import import statfi, energiateollisuus

fuel_emission_factors = statfi.get_fuel_classification(include_units=True)
energy_production = statfi.get_energy_production_stats()
et = energiateollisuus.get_electricity_production_hourly_data(include_units=False)
monthly_electricity_data = energiateollisuus.get_electricity_monthly_data()
fg = load_datasets(INPUT_DATASETS, include_units=False)

import plotly
from plotly.offline import iplot
import plotly.graph_objs as go
import cufflinks as cf
import aplans_graphs

plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)
# -


from quilt.data.jyrjola import aluesarjat

df = aluesarjat.hginseutu_va_ve01_vaestoennuste_pks()



# ## Fuel emission coefficients
#
# An emission coefficient is a value that relates the quantity of pollutants released to the atmosphere with an associated activity. Each of the fuel has a unit emissions coefficient that is based on the amount of gases with [global warming potential](https://en.wikipedia.org/wiki/Global_warming_potential) that are emitted. The emission coefficient is converted to match the mass of $\ce{CO2}$ that would produce the equivalent amount of GWP.
#
# \begin{equation*}
# GWP(x) = \frac
#   {\int_0^{TH} a_x \cdot \left[ x(t) \right] dt}
#   {\int_0^{TH} a_r \cdot \left[ r(t) \right] dt}
# \end{equation*}
#
# where $x$ refers to the target gas and $r$ to the reference gas, here $\ce{CO2}$. For this analysis, the [emission coefficients](https://www.stat.fi/tup/khkinv/khkaasut_polttoaineluokitus.html) for fuels are provided by Statistics Finland.

df = fuel_emission_factors[['name', 'co2e_emission_factor']]
df.co2e_emission_factor = df.co2e_emission_factor.pint.m
df.set_index('name', inplace=True)
df.iplot(kind='bar')

fuel_codes = energy_production['Fuel code'].unique()
df = fuel_emission_factors.loc[fuel_emission_factors['code'].isin(fuel_codes), ['name_en', 'co2e_emission_factor']]
df['co2e_emission_factor'] = df['co2e_emission_factor'].pint.m
df = df.set_index('name_en')
df.iplot(kind='bar', yTitle='g CO2e. / kWh', title='GHG emission coefficients for certain fuels')

# ## Efficiency method
#
# In combined heat and power (CHP) production, emissions are allocated to heat and electricity production according to a chosen method. Here we use the [efficiency method](https://ghgprotocol.org/sites/default/files/CHP_guidance_v1.0.pdf), where  emissions are allocated based on the energy inputs used to produce the separate heat and electricity products. This method uses assumed energy generation efficiency factors of alternative production methods.
#
# First, the fuel consumptions of the alternative production method are calculated:
#
# > $F_{e}^{'}=\frac{E_e}{\eta_e}$
# >
# > $F_{h}^{'}=\frac{E_h}{\eta_h}$
#
# where<br>
# >$F_{e}^{'}$ = fuel consumption of alternate separate electricity production (conventional condensing method)<br>
# >$F_{h}^{'}$ = fuel consumption of alternate heat production (boiler method)<br>
# >$E_e$ = electricity produced in CHP<br>
# >$E_h$ = heat produced in CHP<br>
# >${\eta_e}$ = efficiency of separate electricity production (39 %)<br>
# >${\eta_h}$ = efficiency of separate heat production (90 %)<br>
#
# The measured fuel consumption is then allocated according to the fuel consumptions calculated for the alternate production methods:
#
# > $F_{e} = \frac{F_{e}^{'}}{F_{e}^{'}+F_{h}^{'}} \cdot F$
# >
# > $F_{h} = \frac{F_{h}^{'}}{F_{e}^{'}+F_{h}^{'}} \cdot F$
#
# where<br>
# >$F_{e}$ = fuel consumption allocated to electricity production<br>
# >$F_{h}$ = fuel consumption allocated to heat production<br>
# >$F$ = total fuel consumption in CHP<br>

# +
df = energy_production.set_index('Year')

CHP_METHODS = ['CHP/district heat', 'CHP/industry', 'CHP/small-scale CHP']
HEAT_PRODUCTION = ['Production of district heat', 'Production of industrial steam']

chp = df[df.Method.isin(CHP_METHODS)]
fuel_consumption = chp[(chp.Unit == 'TJ')]['Energy'].groupby(level=0).sum()
electricity = chp[(chp['Energy source'] == 'Net supply')]['Energy'].groupby(level=0).sum()
heat = chp[chp['Energy source'].isin(HEAT_PRODUCTION)]['Energy'].groupby(level=0).sum()

alternate_e_fuel = electricity / 0.39
alternate_h_fuel = heat / 0.90
electricity_fuel_share = alternate_e_fuel / (alternate_e_fuel + alternate_h_fuel)

fuel_consumption.name = ('Fuel consumption', 'GJ')
electricity.name = ('Electricity production', 'TWh')
heat.name = ('Heat production', 'TWh')
electricity_fuel_share.name = 'Electricity fuel share'

df = pd.merge(left=fuel_consumption, right=electricity, left_index=True, right_index=True)
df = pd.merge(left=df, right=heat, left_index=True, right_index=True)
df = (df / 1000).astype(int)
df['Share of fuel for electricity'] = (electricity_fuel_share * 100).round(1).astype(str) + ' %'
df[df.index > 2010]
# -

fuels = fuel_emission_factors[['code', 'co2e_emission_factor']].set_index('code')
df = energy_production.merge(fuels, how='left', left_on='Fuel code', right_index=True)
production = df[df.Unit == 'TJ']
df['Emissions'] = production['Energy'].astype('pint[TJ]') * production['co2e_emission_factor']
df['Emissions'] = df['Emissions'].pint.to('Mt').pint.m
df = df.merge(electricity_fuel_share, how='left', left_on='Year', right_index=True)
display(df[(df.Year == 2016) & (df.Emissions) & (df['Energy source'] == 'Hard coal')])
# Only CHP methods should use the efficiency method for emissions allocation
df.loc[~df.Method.isin(CHP_METHODS), 'Electricity fuel share'] = 0
df.loc[df.Method.isin(['Conventional condensing', 'Electricity fuel share'])] = 1
df['Electricity emissions'] = df['Emissions'] * df['Electricity fuel share']
#df.groupby(['Year', 'Energy source'])['Electricity emissions'].sum()




# +
# Copied from https://energia.fi/files/1414/a_Sahkontuotannon_kk_polttoaineet_tammi.pdf
ELECTRICITY_PRODUCTION_BY_ENERGY_SOURCE = {
    'Bio': [1115, 1064, 1125, 983, 806, 701, 718, 682, 744, 1006, 1131, 1177, 1209, 1183, 1253, 1002, 784, 775, 842, 884, 905, 1105, 1199, 1375, 1405],
    'Others': [87, 84, 81, 75, 66, 69, 77, 63, 57, 70, 77, 83, 83, 72, 76, 79, 73, 68, 81, 74, 68, 74, 83, 100, 95],
    'Peat': [337, 335, 293, 256, 210, 98, 74, 85, 110, 238, 285, 314, 361, 363, 377, 257, 158, 111, 121, 161, 191, 273, 316, 358, 378],
    'Natural gas': [656, 570, 452, 281, 108, 38, 44, 65, 92, 176, 211, 435, 533, 647, 672, 210, 26, 26, 55, 235, 309, 292, 361, 563, 752],
    'Oil': [18, 17, 17, 14, 12, 9, 9, 10, 13, 16, 16, 17, 18, 18, 21, 16, 10, 10, 13, 16, 18, 19, 20, 20, 21],
    'Coal': [690, 651, 718, 570, 548, 339, 243, 207, 328, 532, 533, 570, 617, 614, 840, 519, 295, 261, 418, 220, 261, 451, 588, 703, 775],
    'Solar': [None, 0, 2, 3, 6, 7, 11, 8, 5, 2, 1, 0, 1, 2, 7, 14, 23, 27, 39, 27, 14, 7, 2, 0, 1],
    'Wind': [543, 292, 487, 357, 323, 278, 236, 408, 278, 433, 593, 567, 499, 459, 490, 333, 443, 495, 275, 486, 617, 609, 689, 463, 463],
    'Hydro': [1155, 1002, 1062, 936, 1329, 1358, 1236, 1111, 1456, 1363, 1290, 1312, 1481, 1382, 1324, 1251, 1614, 923, 770, 678, 766, 1001, 974, 980, 1115],
    'Nuclear': [2001, 1859, 2064, 1802, 1518, 1334, 1620, 1723, 1751, 1838, 1997, 2066, 2072, 1872, 2060, 1815, 1511, 1487, 1906, 1697, 1595, 1808, 1994, 2073, 2072],
    'Total': [6602, 5875, 6300, 5276, 4926, 4232, 4267, 4363, 4832, 5675, 6134, 6542, 6874, 6613, 7120, 5494, 4937, 4184, 4520, 4479, 4744, 5640, 6224, 6635, 7078],
}

SEPARATE_THERMAL_POWER_BY_FUEL = {
    'Bio': [99, 119, 92, 61, 69, 35, 66, 70, 73, 135, 115, 118, 140, 150, 157, 105, 52, 86, 152, 164, 148, 161, 156, 157, 154],
    'Others': [19, 17, 16, 12, 12, 13, 16, 14, 15, 20, 15, 19, 19, 17, 16, 12, 15, 20, 21, 17, 18, 21, 19, 19, 16],
    'Peat': [59, 68, 37, 22, 39, 7, 14, 18, 12, 48, 46, 48, 63, 69, 79, 54, 44, 37, 62, 95, 84, 80, 81, 72, 67],
    'Natural gas': [1, 1, 0, 1, 1, 0, 1, 0, 1, 1, 5, 1, 2, 1, 0, 1, 1, 0, 13, 39, 39, 1, 4, 1, 0],
    'Oil': [6, 6, 5, 3, 4, 4, 4, 5, 7, 7, 5, 5, 6, 6, 8, 7, 5, 5, 8, 11, 11, 10, 8, 6, 7],
    'Coal': [142, 160, 185, 80, 226, 177, 124, 78, 106, 103, 82, 87, 125, 168, 323, 115, 95, 125, 317, 186, 138, 118, 160, 180, 232],
    'Total': [325, 373, 337, 181, 351, 235, 225, 186, 213, 314, 268, 279, 355, 411, 584, 294, 211, 273, 572, 512, 438, 391, 429, 435, 476],
}

CHP_POWER_BY_FUEL = {
    'Bio': [1015, 945, 1033, 921, 738, 667, 653, 612, 672, 871, 1016, 1059, 1069, 1033, 1096, 897, 732, 689, 691, 721, 756, 944, 1042, 1218, 1251],
    'Others': [68, 67, 64, 63, 53, 56, 61, 49, 42, 50, 62, 64, 64, 55, 59, 66, 58, 49, 60, 56, 50, 53, 63, 81, 79],
    'Peat': [278, 267, 256, 233, 171, 91, 60, 67, 98, 190, 239, 266, 298, 294, 298, 203, 115, 74, 59, 66, 107, 193, 235, 286, 311],
    'Natural gas': [656, 569, 452, 280, 107, 38, 43, 65, 91, 175, 206, 434, 532, 646, 671, 209, 25, 26, 42, 196, 271, 291, 357, 562, 751],
    'Oil': [13, 11, 12, 10, 8, 6, 5, 5, 6, 9, 11, 11, 12, 12, 13, 9, 6, 5, 5, 5, 6, 10, 12, 13, 14],
    'Coal': [548, 490, 532, 490, 322, 162, 119, 129, 221, 429, 451, 482, 492, 446, 517, 404, 199, 137, 102, 34, 124, 333, 427, 523, 543],
    'Total': [2578, 2349, 2349, 1998, 1399, 1020, 940, 927, 1130, 1725, 1985, 2317, 2466, 2487, 2654, 1788, 1135, 979, 958, 1078, 1314, 1823, 2136, 2683, 2951],
}

df = pd.DataFrame(CHP_POWER_BY_FUEL, index=pd.date_range('2017-01', '2019-01', freq='MS'))
burned_fuels = ['Oil', 'Others', 'Bio', 'Peat', 'Natural gas', 'Coal']
df['total_burned'] = df.loc[:, burned_fuels].sum(axis=1)

traces = []
for fuel in burned_fuels:
    trace = go.Bar(x=df.index, y=df[fuel] / df.total_burned, name=fuel)
    traces.append(trace)

iplot(go.Figure(
    data=traces,
    layout=go.Layout(
        barmode='stack',
        yaxis=dict(tickformat= ',.0%'),
        title='Energy production in CHP by fuel',
    ),
))

# +
import camelot
import unicodedata

# #%matplotlib widget
regions = '120,220,750,105'
if True:
    regions = [int(x) for x in regions.split(',')]

PAGES = (
    ('Separate Thermal Power', 11),
    ('CHP', 12)
)

for energy, page in PAGES:
    tables = camelot.read_pdf('data/a_Sahkontuotannon_kk_polttoaineet_tammi.pdf', flavor='stream', pages=str(page), table_regions=[regions])
    df = tables[0].df
    df = df.iloc[3:].copy()
    df['fuel'] = df[0].apply(lambda x: x.replace('\u2010', '-').replace('\u00a0', ' ').split(' - ')[1])
    df.drop(columns=[0,26,27,28,29,30], inplace=True)
    df = df.set_index('fuel').T.set_index(pd.date_range('2017-01', '2019-01', freq='MS'))
    df.drop(columns='Total', inplace=True)
    for col in df.columns:
        df[col] = pd.to_numeric(df[col]).astype('pint[TJ]')
    df.name = energy
    if energy == 'CHP':
        chp_fuel_use = df
    else:
        separate_fuel_use = df

# +
import pint

emission_map = {
    'Oil': 'Muut raskaat öljyt',
    'Coal': 'Kivihiili',
    'Natural gas': 'Maakaasu',
    'Peat': 'Jyrsinturve',
    # Emissions from biofuels is defined to be zero
    #'Bio': 'Muut kasviperäiset polttoaineet'
}

out = {}
for cat, fuel_name in emission_map.items():
    out[cat] = fuel_emission_factors[fuel_emission_factors['name'] == fuel_name].co2e_emission_factor.pint.m.sum()
out['Others'] = 116  # Estimate to get the emissions align with the Energiateollisuus calculations

def get_emissions(fuel_use):
    df = fuel_use
    emission_factors = [out.get(x, 0) for x in df.columns]
    emissions = df.mul(emission_factors)
    for col in emissions.columns:
        emissions[col] = emissions[col].pint.m
    emissions = emissions.sum(axis=1)
    emissions.name = 'emissions'
    return emissions

chp_emissions = get_emissions(chp_fuel_use)
separate_emissions = get_emissions(separate_fuel_use)
total_emissions = chp_emissions + separate_emissions

def calculate_co2e_emissions_factor(emissions, production):
    production = production.copy()
    production.name = 'production'
    df = pd.concat([emissions, production], axis=1).dropna().sort_index()

    return df.emissions / df.production

emission_factors = (
    ('Combined electricity and heat production (CHP)', 'CHP', chp_emissions, monthly_electricity_data['PRODUCTION/Conv. thermal power/Co-generation, CHP']),
    ('Condense thermal power', 'Separate Thermal Power', separate_emissions, monthly_electricity_data['PRODUCTION/Conv. thermal power/Condense etc./conventional']),
    ('Total' , 'Total', total_emissions, monthly_electricity_data['PRODUCTION']),
)

co2e_emission_factors = None

for name, col_name, emissions, production in emission_factors:
    co2ef = calculate_co2e_emissions_factor(emissions, production)
    co2ef.name = col_name
    co2ef.iplot(
        kind='line',
        layout=dict(
            yaxis=dict(rangemode='tozero', title='g CO2e/kWh', tickformat=',.0f', fixedrange=True),
            xaxis=dict(title='Month', fixedrange=True),
            title='CO2e emissions factor: %s' % name
        ),
    )
    if co2e_emission_factors is None:
        co2e_emission_factors = pd.DataFrame(co2ef)
    else:
        co2e_emission_factors = co2e_emission_factors.join(co2ef)

monthly_co2e_emission_factors = co2e_emission_factors.tz_localize('Europe/Helsinki')

# +
fg_to_et_map = {
    'Production': 'electricity_production',
    'CHP-District heating': 'chp_electricity_generation',
    'CHP-Industry': 'industrial_chp_electricity_generation',
    'Hydro Power': 'hydroelectric_power_generation',
    'Wind Power': 'wind_power_generation',
    'Nuclear Power': 'nuclear_power_generation',
    'Separate Thermal Power': 'condensing_power_generation',
}

et_monthly_to_et_hourly_map = {
    'Production': 'PRODUCTION',
    'CHP-District heating': 'PRODUCTION/Conv. thermal power/Co-generation, CHP/district heating',
    'CHP-Industry': 'PRODUCTION/Conv. thermal power/Co-generation, CHP/industry',
    'Hydro Power': 'PRODUCTION/Hydro power',
    'Wind Power': 'PRODUCTION/Wind power',
    'Nuclear Power': 'PRODUCTION/Nuclear power',
    'Separate Thermal Power': 'PRODUCTION/Conv. thermal power/Condense etc./industry',    
}

monthly = fg.groupby(pd.Grouper(freq='MS')).sum() / 1000
cols = []
fgmm = monthly.rename(columns={x[1]: x[0] for x in fg_to_et_map.items()}).loc[:, fg_to_et_map.keys()]
etmm = monthly_electricity_data.rename(columns={x[1]: x[0] for x in et_monthly_to_et_hourly_map.items()}).loc[:, et_monthly_to_et_hourly_map.keys()]

fgmm = fgmm.loc[(fgmm.index >= '2017-10') & (fgmm.index < '2019-02')]
etmm = etmm.loc[(etmm.index >= '2017-10') & (etmm.index < '2019')]

#df = pd.DataFrame(fg.electricity_production)
#for et_key, fg_key in energy_map.items():
#    diff = both[et_key] - both[fg_key]
#    diff.name = et_key
#    # display(pd.DataFrame(diff))

etmm.index = etmm.index.tz_localize('Europe/Helsinki')
# -

df = monthly_co2e_emission_factors[(monthly_co2e_emission_factors.index >= '2017-10') & (monthly_co2e_emission_factors.index < '2019-02')]
chp_co2e = df.CHP
sep_co2e = df['Separate Thermal Power']

# +
from scipy.optimize import least_squares

def determine_separate_thermal_power_share():
    chp_target = [float(x) for x in '556 502 486 412 262 136 103 115 177 336 378 463 507 512 553 342 160 111  95 105 183 324 405 526 595'.split()]
    sep_target = [float(x) for x in '226 256 251 119 295 209 161 114 141 175 149 156 213 264 446 191 158 186 433 341 277 226 273 281 333'.split()]
    chp_target = chp_target[9:]
    sep_target = sep_target[9:]

    def check_func(x):
        dh = fgmm['CHP-District heating']
        ind = fgmm['CHP-Industry']
        sep = fgmm['Separate Thermal Power']
        chp_emissions = (x[0] * dh + x[1] * ind) * chp_co2e / 1000
        sep_emissions = ((1 - x[0]) * dh + (1 - x[1]) * ind + x[2] * sep) * sep_co2e / 1000

        ret = np.absolute((chp_emissions - chp_target).values) + np.absolute((sep_emissions - sep_target).values)
        return ret

    ret = least_squares(check_func, [0.1] * 3, bounds=[0, 1])
    return ret.x

# f = determine_separate_thermal_power_share()

# about 26% of CHP-Industry is actually separate thermal power??
f = np.array([1, 0.737, 1])

df['chp_electricity_generation'] = f[0] * chp_co2e + (1 - f[0]) * sep_co2e
df['industrial_chp_electricity_generation'] = f[1] * chp_co2e + (1 - f[1]) * sep_co2e
df['other_electricity_generation'] = f[2] * sep_co2e

hourly_emission_factors = df[['chp_electricity_generation', 'industrial_chp_electricity_generation', 'other_electricity_generation']]\
    .resample('1h').asfreq().interpolate()

# Fill the emission factor series into the future by a year
first = hourly_emission_factors.index[0]
last = hourly_emission_factors.index[-1]
last += timedelta(days=365)
hourly_emission_factors = hourly_emission_factors.reindex(pd.date_range(start=first, end=last, freq='1h')).fillna(method='pad')

df = fg[['electricity_production', 'electricity_net_export', 'chp_electricity_generation', 'industrial_chp_electricity_generation', 'other_electricity_generation']]
df = df.loc[df.index >= '2017-10'].copy()
# If we are exporting electricity, do not deduct that from net production
df['electricity_net_export'][df.electricity_net_export > 0] = 0
# Total = production + import
df['supply'] = df['electricity_production'] - df['electricity_net_export']

GHG_EMITTING_METHODS = ['chp_electricity_generation', 'industrial_chp_electricity_generation', 'other_electricity_generation']
for key in GHG_EMITTING_METHODS:
    co2e_key = '%s_co2e' % key
    df[co2e_key] = hourly_emission_factors[key] * df[key]

df['co2e_emissions'] = df[['%s_co2e' % key for key in GHG_EMITTING_METHODS]].sum(axis=1) 
df['co2e_emission_factor'] = df['co2e_emissions'] / df['supply']
df['co2e_emission_factor'] *= 1.25  # MAGIC multiplier to get the emission factors to match


layout = go.Layout(
    yaxis=dict(rangemode='tozero', title='g (CO₂e)/kWh', tickformat=',.0f', fixedrange=True),
    title="Suomen sähkönhankinnan khk-päästökerroin",
)


fig_series = df['co2e_emission_factor'].dropna()
fig = go.Figure(data=[go.Scatter(dict(x=fig_series.index, y=fig_series))], layout=layout)
plotly.offline.iplot(fig)
#aplans_graphs.post_graph(fig, 80)

# -



# +
monthly = fg.groupby(pd.Grouper(freq='YS')).sum() / 1000
#fgmm = monthly.rename(columns={x[1]: x[0] for x in fg_to_et_map.items()}).loc[:, fg_to_et_map.keys()]
#fgmm.Production - fgmm['CHP-District heating'] - fgmm['CHP-Industry'] - fgmm['Hydro Power'] - fgmm['Wind Power'] - fgmm['Nuclear Power'] - fgmm['Separate Thermal Power']

#diff = monthly['electricity_production'] - monthly['chp_electricity_generation'] - monthly['industrial_chp_electricity_generation'] - monthly['nuclear_power_generation'] - monthly['hydroelectric_power_generation'] - monthly['wind_power_generation'] - monthly['other_electricity_generation'] - monthly['condensing_power_generation']
#monthly[monthly.index.year == 2016]
#monthly[monthly.index.year == 2016]
monthly[['condensing_power_generation', 'other_electricity_generation']]
