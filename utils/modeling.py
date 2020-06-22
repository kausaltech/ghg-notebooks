import pandas as pd
import scipy


def generate_forecast_series(historical_series, year_until):
    s = historical_series
    start_year = s.index.min()
    res = scipy.stats.linregress(s.index, s)

    years = list(range(start_year, year_until + 1))
    predictions = pd.Series([res.intercept + res.slope * year for year in years], index=years)
    return predictions
