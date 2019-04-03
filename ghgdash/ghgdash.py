# -*- coding: utf-8 -*-
import dash
import dash_table
from dash_table.Format import Format, Scheme
import dash_core_components as dcc
import dash_html_components as html
import dash_bootstrap_components as dbc
from dash.dependencies import Input, Output
import pandas as pd
import plotly.graph_objs as go
# + {}
from utils.quilt import load_datasets

from .variables import get_variable, set_variable

INPUT_DATASETS = [
    'jyrjola/aluesarjat/hginseutu_va_ve01_vaestoennuste_pks',
    'jyrjola/aluesarjat/z02UM_Rakennukset_lammitys',
    'jyrjola/hsy/pks_khk_paastot',
]

# Datasets
population_forecast = None
buildings_by_heating_method = None
ghg_emissions = None


# Adjusted datasets
def get_population_forecast():
    correction_perc = get_variable('population_forecast_correction')
    target_year = get_variable('target_year')

    df = population_forecast[population_forecast.index <= target_year].copy()
    forecast = df.loc[df.Forecast]
    n_years = forecast.index.max() - forecast.index.min()
    base = (1 + (correction_perc / 100)) ** (1 / n_years)
    multipliers = [base ** year for year in range(n_years + 1)]
    m_series = pd.Series(multipliers, index=forecast.index)
    df.loc[df.Forecast, 'Population'] *= m_series
    df.Population = df.Population.astype(int)
    return df


# +
def process_population_forecast_dataset(df):
    df = df.copy()
    df.Vuosi = df.Vuosi.astype(int)
    df.value = df.value.astype(int)
    df.loc[df.Vuosi <= 2018, 'Forecast'] = False
    df.loc[df.Vuosi > 2018, 'Forecast'] = True
    df = df.query("""
        Alue == '{municipality}' & Laadintavuosi == 'Laadittu 2018' &
        Vaihtoehto == 'Perusvaihtoehto' & Sukupuoli == 'Molemmat sukupuolet'
    """.replace('\n', '').format(municipality=get_variable('municipality_name')))
    df = df.set_index('Vuosi')
    df = df.query("Ikä == 'Väestö yhteensä'")[['value', 'Forecast']].copy()
    df.rename(columns=dict(value='Population'), inplace=True)
    return df


def process_buildings_dataset(df):
    df = df[df.Alue == get_variable('municipality_name')].drop(columns='Alue')
    col = df['Käyttötarkoitus ja kerrosluku']
    # Drop all the rows that are just sums of other rows
    sum_labels = ['Kaikki rakennukset', 'Asuinrakennukset yhteensä', 'Muut rakennukset yhteensä']
    df = df[~col.isin(sum_labels)]
    df = df[~((df['Lämmitystapa'] == 'Yhteensä') | (df['Lämmitysaine'] == 'Yhteensä'))]

    col_list = list(df.columns)
    col_list.remove('value')
    df = df.set_index(col_list)['value'].unstack('Yksikkö').reset_index()
    df.Vuosi = df.Vuosi.astype(int)
    df = df.set_index('Vuosi')
    df.columns.name = None

    return df

def process_ghg_emissions_dataset(df):
    df = df[df.Kaupunki == 'Helsinki'].drop(columns='Kaupunki')
    df = df.set_index('Vuosi')
    return df


def process_input_datasets():
    global population_forecast
    global buildings_by_heating_method
    global ghg_emissions
    
    pop_in, buildings_in, ghg_in = load_datasets(INPUT_DATASETS)

    population_forecast = process_population_forecast_dataset(pop_in)
    buildings_by_heating_method = process_buildings_dataset(buildings_in)
    ghg_emissions = process_ghg_emissions_dataset(ghg_in)


process_input_datasets()


# -

def generate_population_forecast_graph(pop_df):
    hist_df = pop_df.query('~Forecast')
    hist = go.Scatter(
        x=hist_df.index,
        y=hist_df.Population,
        mode='lines',
        name='Väkiluku',
        line=dict(
            color='#9fc9eb',
        )
    )

    forecast_df = pop_df.query('Forecast')
    forecast = go.Scatter(
        x=forecast_df.index,
        y=forecast_df.Population,
        mode='lines',
        name='Väkiluku (enn.)',
        line=dict(
            color='#9fc9eb',
            dash='dash'
        )
    )
    fig = go.Figure(data=[hist, forecast])
    return fig


# +
GHG_SECTOR_MAP = {
    'heating': 'Lämmitys',
    'electricity': 'Sähkö',
    'transport': 'Liikenne',
    'waste_management': 'Jätteiden käsittely',
    'industry': 'Teollisuus ja työkoneet',
}


def get_ghg_emissions_forecast():
    target_year = get_variable('target_year')
    reference_year = get_variable('ghg_reductions_reference_year')
    reduction_percentage = get_variable('ghg_reductions_percentage_in_target_year')
    sector_weights = get_variable('ghg_reductions_weights')

    df = ghg_emissions.reset_index().groupby(['Vuosi', 'Sektori1'])['Päästöt'].sum().reset_index().set_index('Vuosi')

    ref_emissions = df[df.index == reference_year]['Päästöt'].sum()
    target_emissions = ref_emissions * (1 - (reduction_percentage / 100))
    last_emissions = dict(df.loc[[df.index.max()], ['Sektori1', 'Päästöt']].reset_index().set_index('Sektori1')['Päästöt'])    

    other_sectors = [s for s in last_emissions.keys() if s not in GHG_SECTOR_MAP.values()]

    main_sector_emissions = sum([val for key, val in last_emissions.items() if key in GHG_SECTOR_MAP.values()])
    emission_shares = {sector_id: last_emissions[sector_name] / main_sector_emissions for sector_id, sector_name in GHG_SECTOR_MAP.items()}
    main_sector_target_emissions = target_emissions - sum([last_emissions[s] for s in other_sectors])

    target_year_emissions = {}

    weight_sum = sum(sector_weights.values())
    for sector_id, sector_name in GHG_SECTOR_MAP.items():
        weight = (sector_weights[sector_id] / weight_sum) * len(sector_weights)
        emission_shares[sector_id] /= weight

    sum_shares = sum(emission_shares.values())
    for key, val in emission_shares.items():
        emission_shares[key] = val / sum_shares

    for sector_id, sector_name in GHG_SECTOR_MAP.items():
        target = main_sector_target_emissions * emission_shares[sector_id]
        target_year_emissions[sector_name] = target

    for sector_name in other_sectors:
        target_year_emissions[sector_name] = last_emissions[sector_name]

    df = df.reset_index().set_index(['Vuosi', 'Sektori1']).unstack('Sektori1')
    df.columns = df.columns.get_level_values(1)
    last_historical_year = df.index.max()
    df.loc[target_year] = [target_year_emissions[x] for x in df.columns]
    df = df.reindex(range(df.index.min(), df.index.max() + 1))
    future = df.loc[df.index >= last_historical_year].interpolate()
    df.update(future)
    df.dropna(inplace=True)
    df.loc[df.index <= last_historical_year, 'Forecast'] = False
    df.loc[df.index > last_historical_year, 'Forecast'] = True
    return df


# -

def generate_buildings_forecast_graph(buildings_df):
    #col_name = 'Käyttötarkoitus ja kerrosluku'
    col_name = 'Lämmitysaine'
    df = buildings_df.reset_index().groupby(['Vuosi', col_name])['Kerrosala'].sum()
    df = df.reset_index().set_index(['Vuosi', col_name])
    df = df.unstack(col_name)
    df.columns = pd.Index([x[1] for x in df.columns.to_flat_index()])
    df /= 1000
    traces = []
    last_year = df.loc[[df.index.max()]]
    columns = last_year.stack().sort_values(ascending=False).index.get_level_values(1).values
    for building_type in columns:
        trace = go.Bar(x=df.index, y=df[building_type], name=building_type)
        traces.append(trace)

    layout = go.Layout(
        barmode='stack',
        yaxis=dict(
            title='1 000 m²',
            hoverformat='.3r',
            separatethousands=True,
        ),
        xaxis=dict(title='Vuosi'),
        title='Kerrosala lämmitysaineen mukaan'
    )
    return go.Figure(data=traces, layout=layout)


app = dash.Dash(__name__, external_stylesheets=[dbc.themes.BOOTSTRAP])
app.css.config.serve_locally = True
app.scripts.config.serve_locally = True

# +
ghg_sliders = []


def generate_ghg_sliders():
    out = []
    for key, val in GHG_SECTOR_MAP.items():
        if val == 'Lämmitys':
            slider_val = 40
        else:
            slider_val = 25
        slider = dcc.Slider(
            id='ghg-%s-slider' % key,
            min=5,
            max=50,
            step=1,
            value=slider_val,
            marks={25: ''},
        )
        out.append(dbc.Col([
            html.Strong('%s' % val),
            slider
        ], md=12, className='mb-4'))
        ghg_sliders.append(slider)

    return dbc.Row(out)


def find_consecutive_start(values):
    last_val = start_val = values[0]
    for val in values[1:]:
        if val - last_val != 1:
            start_val = val
        last_val = val
    return start_val


def generate_ghg_emission_graph(df):
    COLORS = {
        'Lämmitys': '#3E9FA8',
        'Sähkö': '#9FD9DA',
        'Liikenne': '#E9A5CA',
        'Teollisuus ja työkoneet': '#E281B6',
        'Jätteiden käsittely': '#9E266D',
        'Maatalous': '#680D48',
    }

    start_year = find_consecutive_start(df.index.unique())

    hist_df = df.query('~Forecast & index > %s' % start_year)

    latest_year = hist_df.loc[hist_df.index.max()]
    data_columns = list(latest_year.sort_values(ascending=False).index)
    data_columns.remove('Forecast')

    hist_traces = [go.Scatter(
        x=hist_df.index,
        y=hist_df[sector],
        mode='lines',
        name=sector,
        line=dict(
            color=COLORS[sector]
        )
    ) for sector in data_columns]

    forecast_df = df.query('Forecast | index == %s' % hist_df.index.max())
    forecast_traces = [go.Scatter(
        x=forecast_df.index,
        y=forecast_df[sector],
        mode='lines',
        name=sector,
        line=dict(
            color=COLORS[sector],
            dash='dash',
        ),
        showlegend=False,
    ) for sector in data_columns]

    layout = go.Layout(
        yaxis=dict(
            hoverformat='.3r',
            separatethousands=True,
            title='KHK-päästöt (kt CO₂-ekv.)'
        ),
        margin=go.layout.Margin(
            t=0,
            r=0,
        ),
        legend=dict(
            x=0.7,
            y=1,
            traceorder='normal',
            bgcolor='#fff',
        ),
    )

    fig = go.Figure(data=hist_traces + forecast_traces, layout=layout)
    return fig


# +
population_tab_content = dbc.Card(
    dbc.CardBody(
        [
            html.H5('Väestöennusteen korjausprosentti'),
            html.Div([
                dcc.Slider(
                    id='population-slider',
                    min=-20,
                    max=20,
                    step=5,
                    value=0,
                    marks={x: '%d %%' % x for x in range(-20, 20 + 1, 5)},
                ),
            ], style={'marginBottom': 25}),
            html.Div([
                html.P(children=[
                    'Väestön määrä vuonna %s: ' % get_variable('target_year'),
                    html.Strong(id='population-count-target-year')
                ]),
            ]),
            dcc.Graph(
                id='population-graph',
                config={
                    'displayModeBar': False,
                    'showLink': False,
                }
            ),
        ]
    ),
    className="mt-3",
)


buildings_tab_content = dbc.Card(
    dbc.CardBody(
        [
            html.H5('Asuinrakennusalan korjausprosentti'),
            html.Div([
                dcc.Slider(
                    id='residential-buildings-slider',
                    min=-20,
                    max=20,
                    step=5,
                    value=0,
                    marks={x: '%d %%' % x for x in range(-20, 20 + 1, 5)},
                ),
            ], style={'marginBottom': 25}),
            html.Div([
                html.P(children=[
                    'Asuinrakennuskerrosalaa vuonna %s: ' % get_variable('target_year'),
                    html.Strong(id='residential-building-area-target-year')
                ]),
            ]),
            dcc.Graph(
                id='buildings-graph',
            ),
        ]
    ),
    className="mt-3",
)


app.layout = dbc.Container([
    dbc.Row([
        dbc.Col([
            html.H2('Kasvihuonekaasupäästöt', style=dict(marginBottom='1em')),
            html.Div(id='ghg-emissions-table-container'),
        ])
    ]),
    dbc.Row([
        dbc.Col([
            html.Div(generate_ghg_sliders(), id='ghg-sliders'),
        ], md=4),
        dbc.Col([
            dcc.Graph(
                id='ghg-emissions-graph',
                config={
                    'displayModeBar': False,
                    'showLink': False,
                }
            ),
        ], md=8),
    ], className='mt-4'),
    dbc.Row([
        dbc.Col([
            html.H2('Yleiset oletukset'),
            dbc.Tabs(id="general-assumptions-tabs", children=[
                dbc.Tab(population_tab_content, label='Väestö'),
                dbc.Tab(buildings_tab_content, label='Rakennuskanta'),
                dbc.Tab(label='Työpaikat'),
            ]),
        ]),
    ]),
])


@app.callback(
    [Output('population-graph', 'figure'), Output('population-count-target-year', 'children')],
    [Input('population-slider', 'value')])
def population_callback(value):
    set_variable('population_forecast_correction', value)
    pop_df = get_population_forecast()
    pop_in_target_year = pop_df.loc[[get_variable('target_year')]].Population
    fig = generate_population_forecast_graph(pop_df)

    return fig, pop_in_target_year.round()


@app.callback(
    [Output('buildings-graph', 'figure'), Output('residential-building-area-target-year', 'children')],
    [Input('residential-buildings-slider', 'value')])
def buildings_callback(value):
    df = buildings_by_heating_method
    fig = generate_buildings_forecast_graph(df)

    return fig, None


@app.callback(
    [Output('ghg-emissions-graph', 'figure'), Output('ghg-emissions-table-container', 'children')],
    [Input(slider.id, 'value') for slider in ghg_sliders])
def ghg_slider_callback(*values):
    sectors = [x.id.split('-')[1] for x in ghg_sliders]
    new_values = {s: val for s, val in zip(sectors, values)}
    set_variable('ghg_reductions_weights', new_values)
    df = get_ghg_emissions_forecast()
    fig = generate_ghg_emission_graph(df)

    df['Yhteensä'] = df.sum(axis=1)
    last_hist_year = df[~df.Forecast].index.max()
    data_columns = list(df.loc[df.index == last_hist_year].stack().sort_values(ascending=False).index.get_level_values(1))

    data_columns.remove('Forecast')
    data_columns.insert(0, 'Vuosi')
    data_columns.remove('Yhteensä')
    data_columns.append('Yhteensä')
    print(data_columns)

    last_forecast_year = df[df.Forecast].index.max()
    table_df = df.loc[df.index.isin([last_hist_year, last_forecast_year - 5, last_forecast_year - 10, last_forecast_year])]
    table_data = table_df.reset_index().to_dict('rows')
    table_cols = []
    for col_name in data_columns:
        col = dict(id=col_name, name=col_name)
        if col_name == 'Vuosi':
            pass
        else:
            col['type'] = 'numeric'
            col['format'] = Format(precision=0, scheme=Scheme.fixed)
        table_cols.append(col)
    table = dash_table.DataTable(
        data=table_data,
        columns=table_cols,
        #style_as_list_view=True,
        style_cell={'padding': '5px'},
        style_header={
            'fontWeight': 'bold'
        },
        style_cell_conditional=[
            {
                'if': {'column_id': 'Vuosi'},
                'fontWeight': 'bold',
            }
        ]
    )

    return [fig, table]


# -


if __name__ == '__main__':
    app.run_server(debug=True)


# +
import plotly
import plotly.graph_objs as go
import cufflinks as cf
plotly.offline.init_notebook_mode(connected=True)
cf.set_config_file(offline=True)
#plotly.offline.iplot(fig)


#plotly.offline.iplot(generate_buildings_forecast_graph(buildings_by_heating_method))
