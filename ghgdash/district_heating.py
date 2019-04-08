import math
import pandas as pd
import scipy.stats
from .variables import get_variable
from utils.quilt import load_datasets
import pintpandas  # noqa


INPUT_DATASETS = [
    'jyrjola/energiateollisuus/district_heating_fuel',
    'jyrjola/energiateollisuus/district_heating_production',
    'jyrjola/statfi/fuel_classification'
]

dh_fuel_df, dh_production_df, fuel_classification = load_datasets(INPUT_DATASETS)


def _prepare_fuel_emissions_dataset(fuel_use_df):
    return df


HEAT_PUMP_COL = 'Lämmön talteenotto tai lämpöpumpun tuotanto'
HEAT_DEMAND_COL = 'Käyttö'
FUEL_NET_PRODUCTION_COL = 'Nettotuotanto polttoaineilla'
PRODUCTION_LOSS_COL = 'Verkkohäviöt ja mittauserot'
TOTAL_PRODUCTION_COL = 'Yhteensä'
CHP_ELECTRICITY_PRODUCTION_COL = 'Kaukolämmön tuotantoon liittyvä sähkön nettotuotanto'

ALL_FUEL_PRODUCTION_TOTAL_COL = 'Kaukolämmön ja yhteistuotantosähkön tuotantoon käytetyt polttoaineet yhteensä'


def calculate_district_heating_unit_emissions(fuel_use_df, production_df):
    emissionless_bio = get_variable('bio_is_emissionless')

    fuel_co2 = fuel_classification[['code', 'co2e_emission_factor', 'is_bio']].set_index('code')
    df = fuel_use_df.merge(fuel_co2, how='left', left_on='StatfiFuelCode', right_index=True)
    df.co2e_emission_factor = df.co2e_emission_factor.astype('pint[t/TJ]')
    df.Value = df.Value.astype('pint[GWh]')
    df['Emissions'] = (df.Value * df.co2e_emission_factor).pint.to('tonne').pint.m

    #df = production_df
    #print(df.loc[df.index == 2017])
    #print(df.loc[df.index == 2018])

    if emissionless_bio:
        df.loc[df.is_bio == True, 'Emissions'] = 0  # noqa

    emissions = df.groupby('Year')['Emissions'].sum()
    emissions.name = 'Emissions'

    df = production_df
    heat_production = df[HEAT_DEMAND_COL]
    heat_production.name = 'Heat production'
    chp_electricity_production = df[CHP_ELECTRICITY_PRODUCTION_COL]
    chp_electricity_production.name = 'Electricity production'

    # Determine the CHP alternate production energy consumptions according to the efficiency method
    electricity_production_alternate = chp_electricity_production / 0.39
    heat_production_alternate = heat_production / 0.90
    total = electricity_production_alternate + heat_production_alternate
    heat_share = heat_production_alternate / total
    heat_share.name = 'Fuel share of heat production in CHP'

    heat_demand = df[HEAT_DEMAND_COL]
    heat_demand.name = 'Heat demand'

    heat_pump_prod = production_df[HEAT_PUMP_COL]
    heat_pump_prod.name = 'Production with heat pumps'

    df = pd.concat([heat_demand, chp_electricity_production, heat_share, heat_pump_prod, emissions], axis=1)
    df['Unit emissions'] = df.Emissions * heat_share / heat_demand
    df['Emissions'] /= 1000

    df['District heat consumption emissions'] = heat_demand * df['Unit emissions'] / 1000
    df['Forecast'] = production_df['Forecast']

    return df


def generate_forecast_series(historical_series, year_until):
    s = historical_series
    start_year = s.index.min()
    res = scipy.stats.linregress(s.index, s)

    years = list(range(start_year, year_until + 1))

    # If a linear trend is improbable, make the forecast series just
    # converge to an average.
    if res.pvalue > 0.05:
        df = historical_series.reindex(range(start_year, year_until + 1))
        df[year_until] = historical_series.mean()
        predictions = df.interpolate()
    else:
        predictions = pd.Series([res.intercept + res.slope * year for year in years], index=years)

    return predictions


def generate_production_forecast(production_df, target_year, target_demand_change, target_heat_pump_share):
    df = production_df

    last_year = df.index.max()

    loss_ratio = df[PRODUCTION_LOSS_COL] / df[HEAT_DEMAND_COL]
    last_loss_ratio = loss_ratio.loc[last_year]
    s = generate_forecast_series(loss_ratio, target_year)
    target_loss_ratio = s[s.index.max()]

    # Demand
    last_demand = df[HEAT_DEMAND_COL].loc[last_year]
    s = generate_forecast_series(df[HEAT_DEMAND_COL], target_year)
    target_demand = s[target_year] * (1 + target_demand_change)

    last_heat_pump_share = (df[HEAT_PUMP_COL] / df[HEAT_DEMAND_COL]).loc[last_year]

    df = pd.DataFrame({HEAT_DEMAND_COL: [last_demand, target_demand]}, index=[last_year, target_year])
    df['LossRatio'] = [last_loss_ratio, target_loss_ratio]
    df['HeatpumpShare'] = [last_heat_pump_share, target_heat_pump_share]
    df = df.reindex(range(last_year, target_year + 1)).interpolate().iloc[1:].copy()
    df[HEAT_PUMP_COL] = df.HeatpumpShare * df[HEAT_DEMAND_COL]
    df[PRODUCTION_LOSS_COL] = df.LossRatio * df[HEAT_DEMAND_COL]
    df[TOTAL_PRODUCTION_COL] = df[HEAT_DEMAND_COL] + df[PRODUCTION_LOSS_COL]
    df[FUEL_NET_PRODUCTION_COL] = df[TOTAL_PRODUCTION_COL] - df[HEAT_PUMP_COL]
    # Amount of electricity that's produced in CHP is about 60 % of the total
    # heat demand.
    df[CHP_ELECTRICITY_PRODUCTION_COL] = df[HEAT_DEMAND_COL] * .60
    df = df.drop(columns=['LossRatio', 'HeatpumpShare'])

    return df


def generate_fuel_use_forecast(fuel_df, production_forecast, target_year, target_ratios):
    last_year = fuel_df.Year.max()

    df = fuel_df[fuel_df.Year == last_year]
    df = df.drop(columns='Year').set_index('Quantity')

    # Make sure we have counted all fuels correctly by checking against
    # the incoming data.
    fuels = df.loc[~df.StatfiFuelCode.isna(), 'Value']
    all_fuels_total = fuels.sum()
    assert math.isclose(df.loc[ALL_FUEL_PRODUCTION_TOTAL_COL, 'Value'], all_fuels_total)

    last_fuel_ratios = (fuels / all_fuels_total).to_dict()

    fuel_ratio_sum = sum([val for key, val in target_ratios.items() if key in last_fuel_ratios])
    target_fuel_ratios = {
        fuel: share / fuel_ratio_sum for fuel, share in target_ratios.items() if fuel in last_fuel_ratios
    }
    for key in last_fuel_ratios.keys():
        if key not in target_fuel_ratios:
            target_fuel_ratios[key] = 0

    df = pd.DataFrame([last_fuel_ratios, target_fuel_ratios], [last_year, target_year])
    fuel_ratio_forecast = df.reindex(range(last_year, target_year + 1)).interpolate().iloc[1:]

    # Total fuel energy needed is (heat production + electricity) / 89% (total efficiency)
    total_fuel_needed = (production_forecast[FUEL_NET_PRODUCTION_COL] + production_forecast[CHP_ELECTRICITY_PRODUCTION_COL]) / 0.89
    df = fuel_ratio_forecast.mul(total_fuel_needed, axis='index')
    df.index.name = 'Year'
    df = df.reset_index().melt(id_vars=['Year'], value_name='Value', var_name='Quantity')

    # Re-add the stat.fi fuel code
    fuel_map = fuel_df[['Quantity', 'StatfiFuelCode']].set_index('Quantity')
    fuel_map = fuel_map[~fuel_map.index.duplicated(keep='first')]
    df = df.merge(fuel_map, left_on='Quantity', right_index=True)
    df['Unit'] = 'GWh'

    return df


def get_district_heating_unit_emissions_forecast():
    operator = get_variable('district_heating_operator')
    target_ratios = get_variable('district_heating_target_production_ratios')
    target_year = get_variable('target_year')
    target_demand_change = get_variable('district_heating_target_demand_change')

    assert sum(target_ratios.values()) == 100

    df = dh_fuel_df
    fuel_df = df[df.Operator == operator].drop(columns=['Operator', 'OperatorName'])

    df = dh_production_df
    df = df[df.Operator == operator].drop(columns=['Operator', 'OperatorName'])
    df = df.set_index('Year').drop(columns='Unit').pivot(columns='Quantity', values='Value')

    # Fill in the missing columns from previous years
    df[HEAT_PUMP_COL] = df[HEAT_PUMP_COL].fillna(0)
    df[FUEL_NET_PRODUCTION_COL] = df[TOTAL_PRODUCTION_COL] - df['Osto'] - df[HEAT_PUMP_COL]
    production_df = df

    heat_pump_share = target_ratios.get('Lämpöpumput') / 100
    production_forecast = generate_production_forecast(
        production_df, target_year, target_demand_change / 100, heat_pump_share
    )
    fuel_use_forecast = generate_fuel_use_forecast(fuel_df, production_forecast, target_year, target_ratios)

    production_df['Forecast'] = False
    production_forecast['Forecast'] = True
    production_df = pd.concat([production_df, production_forecast], sort=False).sort_index()

    fuel_df = pd.concat([fuel_df, fuel_use_forecast], sort=False).set_index('Year').sort_index()
    df = calculate_district_heating_unit_emissions(fuel_df, production_df)

    return df
