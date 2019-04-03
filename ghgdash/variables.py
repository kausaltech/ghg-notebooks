# Variables
VARIABLES = {
    'target_year': 2035,
    'population_forecast_correction': 0,  # Percent in target year
    'ghg_reductions_reference_year': 1990,
    'ghg_reductions_percentage_in_target_year': 80,
    'ghg_reductions_weights': {
        'heating': 30,
        'electricity': 30,
        'transport': 30,
        'waste_management': 30,
        'industry': 30,
    },
    'bio_is_emissionless': True,
    'municipality_name': 'Helsinki',

    'district_heating_operator': '005',  # Helen Oy
}


def set_variable(var_name, value):
    assert var_name in VARIABLES
    assert isinstance(value, type(VARIABLES[var_name]))
    VARIABLES[var_name] = value


def get_variable(var_name):
    return VARIABLES[var_name]
