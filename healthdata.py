import os
import pandas as pd
from dvc_pandas import Dataset, Repository
import pint
import pint_pandas
import settings

# Standard names for columns
# Pollutant (for environmental nodes) --> Exposure_agent (from exposure node onward)
# Emission_height (for intake fraction)
# Population_density (for intake fraction)
# Response (for health impact)
# Route (We need this for telling which parameter columns to use)
# Er_function (name of the exposure-response function to use)
# Erf_context (approximate combination of exposure agent, response and possibly other determinants)
# Vehicle
# Metabolic_equivalent
# Weekly_activity
# Default_paradigm (for the default set of TEF values)
# Case_burden
# Case_cost
# NOTE! Parameter names are NOT written with initial capital letter, because they are not words but symbols.

# Syntax for parameter names
# There are two or three parts separated by '_'.
# First part tells the route and thus defines the base unit.
# Second part tells the power n of the parameter unit: base_unit**n where
# the names m3, m2, m1, p0, p1 mean n = -3, -2, -1, 0, 1, respectively.
# Third part is just a number to differentiate parameters with the same unit.

unit_registry = pint.UnitRegistry(
    preprocessors=[
        lambda s: s.replace('%', ' percent '),
    ],
    on_redefinition='raise'
)

# By default, kt is knots, but here kilotonne is the most common
# usage.
del unit_registry._units['kt']
unit_registry.define('kt = kilotonne')
# Mega-kilometers is often used for mileage
unit_registry.define('Mkm = gigameters')
unit_registry.define(pint.unit.UnitDefinition(
    'percent', '%', (), pint.converters.ScaleConverter(0.01)
))
unit_registry.define('incident = [event] = case = cases')
unit_registry.define('disability_adjusted_life_year = [disease_burden] = DALY')
unit_registry.define('Lden = []')
unit_registry.define('microbe = [] = microbes')
unit_registry.define('parts_per_million = mg / kg = ppm')
unit_registry.define('personyear = person * year')
unit_registry.define('euro = [] = EUR')
unit_registry.define('toxic_equivalency_factor = [tef] = TEF')
unit_registry.define('MET = [metabolic_equivalent]')
unit_registry.define('METh = MET * h')

unit_registry.default_format = '~P'
pint.set_application_registry(unit_registry)
pint_pandas.PintType.ureg = unit_registry  # type: ignore

repo = Repository(repo_url='git@github.com:kausaltech/dvctest.git', dvc_remote='kausal-s3')

# Physical activity

df = pd.DataFrame({
    'Vehicle': pd.Series(['walking', 'cycling']),
    'Velocity': pd.Series([5.3, 14.]),
    'Route': pd.Series(['exercise'] * 2),
    'Metabolic_equivalent': pd.Series([4., 6.8]),
    'Weekly_activity': pd.Series([3., 3.]),
    'Erf_context': pd.Series(['walking mortality', 'cycling mortality']),
    'Pollutant': pd.Series(['physical activity'] * 2),
    'Response': pd.Series(['mortality'] * 2),
    'Case_burden': pd.Series([15.84386, 15.84386]),
    'Period': pd.Series([1., 1.]),
    'Er_function': pd.Series(['relative risk'] * 2),
    'exercise_m1': pd.Series([-0.0093653792] * 2),
    'exercise_p1': pd.Series([0., 0]),
    'exercise_p0': pd.Series([0.6, 0.6]),
})

unit = dict({
    'Velocity': 'km / h',
    'Metabolic_equivalent': 'METh / h',
    'Weekly_activity': 'day / week',
    'Case_burden': 'DALY / case',
    'Period': 'a / incident',
    'exercise_m1': 'week / METh',
    'exercise_p1': 'METh / week',
})

metadata = dict({
    'references': {
        'General': 'http://fi.opasnet.org/fi/Liikenteen_terveysvaikutukset ' +
        'Lehtomäki, H., Karvosenoja, N., Paunu, V-V., Korhonen, A., Hänninen, O., ' +
        'Tuomisto, J., Karppinen, A., Kukkonen, J. & Tainio, M. 2021. Liikenteen ' +
        'terveysvaikutukset Suomessa ja suurimmissa kaupungeissa. Suomen ympäristökeskuksen ' +
        'raportteja 16/2021. http://hdl.handle.net/10138/329273',
        'Velocity': 'Kelly ym. 2014',
        'Metabolic_equivalent': 'Kahlmeier, S., Götschi, T., Cavill, N., Castro Fernandez, ' +
        'A., Brand, C., Rojas Rueda, D., Woodcock, J., Kelly, P., Lieb, C., Oja, P., Foster, ' +
        'C., Rutter, H., & Racioppi, F. 2017. Health economic assessment tool (HEAT) for walking ' +
        'and for cy- cling. Methods and user guide on physical activity, air pollution, injuries ' +
        'and carbon impact assessments. World Health Organization. Denmark, Copenhagen. ' +
        'https://www.euro.who.int/__data/assets/pdf_file/0010/352963/Heat.pdf',
        'Weekly_activity': 'Cambridgen yliopisto 2020, Woodcock, J., Tainio, M., Cheshire, J., ' +
        'O’Brien, O. & Goodman, A. 2014. Health effects of the London bicycle sharing system: ' +
        'health impact modelling study. The BMJ 348:g425.',
        'Er_function': 'Kelly, P., Kahlmeier, S., Götschi, T. et al. Systematic review and ' +
        'meta-analysis of reduction in all-cause mortality from walking and cycling and shape ' +
        'of dose response relationship. Int J Behav Nutr Phys Act 11, 132 (2014). ' +
        'https://doi.org/10.1186/s12966-014-0132-x',
        'Case_burden': 'Total YLL / total deaths in Finland according to Lehtomäki et al, 2021.',
        'exercise_m1': 'beta: log(0.9)/(11.25 METh/week)',
        'exercise_p0': 'rrmin: 0.6, the smallest relative risk plausible',
    }
})

ds_act = Dataset(
    df,
    identifier='hia/exposure_response/physical_activity',
    units=unit,
    metadata=metadata)

## Exposure to physical activity

df = pd.DataFrame({
    'Age group': pd.Series(['20-49', '50-64', '65-79', '80-99'] * 2),
    'Vehicle': pd.Series(['walking'] * 4 + ['cycling'] * 4),
    'Active population fraction': pd.Series([0.48100, 0.44356, 0.41227, 0.34643, 0.07830, 0.07586, 0.04940, 0.03214]),
    'Distance': pd.Series([1.06548, 0.95284, 0.95682, 1.06552, 9.64944, 10.25433, 8.76017, 5.20000], dtype='pint[km/d]'),
})

metadata = {
    'references': {
        'General': 'Liikennevirasto. 2018. Henkilöliikennetutkimus 2016. ' +
        'Liikennevirasto, Liikenne ja maankäyttö. Helsinki 2018. Liikenneviraston ' +
        'tilastoja 1/2018. https://julkaisut.vayla.fi/pdf8/lti_2018-01_henkiloliikennetutkimus_2016_web.pdf'
    },
    'notes': {
        'General': 'The values are about the average situation in Finland in 2016 for different age groups.'
    }
}

ds_phys_exposure = Dataset(
    df=df,
    identifier='hia/exposure/physical_activity_fi',
    metadata=metadata
)

# Air pollution

df = pd.DataFrame({
    'Erf_context': pd.Series([
        'PM2.5 mortality',
        'PM2.5 work_days_lost',
        'NOx mortality',
        'PM10 chronic_bronchitis']),
    'Pollutant': pd.Series(['PM2.5', 'PM2.5', 'NOx', 'PM10']),
    'Response': pd.Series(['mortality', 'work_days_lost', 'mortality', 'chronic_bronchitis']),
    'Period': pd.Series([1.] * 4),
    'Route': pd.Series(['inhalation'] * 4),
    'Er_function': pd.Series(['relative risk'] * 4),
    'inhalation_m1': pd.Series([0.007696104114, 0.00449733656, 0.0019802627, 0.0076961941]),
    'inhalation_p1': pd.Series([0.] * 4),
    'inhalation_p0': pd.Series([1.] * 4),
    'Case_burden': pd.Series([10.6, 0.00027, 10.6, 0.99]),
    'Case_cost': pd.Series([0., 152, 0, 62712]),
})
unit = dict({
    'Period': 'year / incident',
    'inhalation_m1': 'm**3 / ug',
    'inhalation_p1': 'ug / m**3',
    'Case_burden': 'DALY / case',
    'Case_cost': 'EUR / case',
})

metadata = {
    'references': {
        'PM2.5 mortality': {
            'General': 'http://en.opasnet.org/w/ERF_of_outdoor_air_pollution',
            'Er_function': 'log(1.08)/10 Chen & Hoek, 2020',
            'Case_burden': 'De Leeuw & Horàlek 2016/5 http://fi.opasnet.org/fi/Kiltova#PAQ2018',
        },
        'PM2.5 work_days_lost': {
            'General': 'http://fi.opasnet.org/fi/Kiltova',
            'Er_function': 'log(1.046)/10 HRAPIE',
            'Case_burden': '0.099 DW * 0.00274 a, Heimtsa & Intarese http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_cost': 'Holland et al., 2014'
        },
        'NOx mortality': {
            'General': 'For NO2 atm.  http://fi.opasnet.org/fi/Kiltova, Atkinson et al., 2017',
            'Er_function': 'log(1.02)/10',
            'Case_burden': 'Same as PM2.5 De Leeuw & Horàlek 2016/5 http://fi.opasnet.org/fi/Kiltova#PAQ2018'
        },
        'PM10 chronic bronchitis': {
            'General': 'http://fi.opasnet.org/fi/Kiltova',
            'Er_function': 'log(1.08)/10 HRAPIE',
            'Case_burden': 'http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_cost': 'Holland et al., 2014'
        }
    },
    'notes': {
        'inhalation_p0': 'Assuming no protective effect even below threshold.',
    }
}

ds_air = Dataset(
    df=df,
    identifier='hia/exposure_response/air_pollution',
    units=unit,
    metadata=metadata)

# Indoor air pollution

df = pd.DataFrame({
    'Erf_exposure': pd.Series(['radon lung_cancer']),
    'Pollutant': pd.Series(['radon']),
    'Response': pd.Series(['lung cancer']),
    'Period': pd.Series([80], dtype='pint[a]'),
    'Route': pd.Series(['inhalationBq']),
    'Er_function': pd.Series(['relative risk']),
    'inhalationBq_m1': pd.Series([0.0015987213636970735], dtype='pint[m**3 / Bq]'),
    'inhalationBq_p1': pd.Series([0.], dtype='pint[Bq / m**3]'),
    'inhalationBq_p0': pd.Series([1.]),
})

metadata = {
    'references': {
        'General': 'http://en.opasnet.org/w/ERF_for_long-term_indoor_exposure_to_radon_and_lung_cancer ' + 
        'http://en.opasnet.org/w/ERFs_of_environmental_pollutants',
        'Er_function': 'log(1.0016) Darby 2005 http://www.bmj.com/cgi/content/full/330/7485/223'
    },
    'notes': {
        'inhalationBq_p0': 'Assuming no protective effect even below threshold',
    }
}

ds_indoor = Dataset(
    df=df,
    identifier='hia/exposure_response/indoor_air',
    metadata=metadata)

# Noise

df = pd.DataFrame({
    'Erf_context': pd.Series([
        'noise highly_annoyed_road',
        'noise highly_annoyed_rail',
        'noise highly_annoyed_air',
        'noise highly_sleep_disturbed_road',
        'noise highly_sleep_disturbed_rail',
        'noise highly_sleep_disturbed_air']),
    'Pollutant': pd.Series(['noise'] * 6),
    'Response': pd.Series(['highly_annoyed'] * 3 + ['highly_sleep_disturbed'] * 3),
    'Period': pd.Series([1.] * 6),
    'Route': pd.Series(['noise'] * 6),
    'Er_function': pd.Series(['polynomial'] * 6),
    'noise_p1': pd.Series([42., 42, 42, 0, 0, 0]),
    'noise_p0': pd.Series([0, 0, 0, 0.208, 0.113, 0.18147]),
    'noise_m1': pd.Series([5.118E-03, 1.695E-03, 2.939E-03, -1.050E-02, -5.500E-03, -9.560E-03]),
    'noise_m2': pd.Series([-1.436E-04, -7.851E-05, 3.932E-04, 1.486E-04, 7.590E-05, 1.482E-04]),
    'noise_m3': pd.Series([9.868E-06, 7.239E-06, -9.199E-07, 0, 0, 0]),
    'Incidence': pd.Series([1.] * 6),
    'Case_burden': pd.Series([0.02, 0.02, 0.02, 0.07, 0.07, 0.07]),
})

unit = dict({
    'Period': 'a / incident',
    'noise_p1': 'Lden',
    'noise_m1': '(Lden)**-1',
    'noise_m2': '(Lden)**-2',
    'noise_m3': '(Lden)**-3',
    'Incidence': 'cases / personyear',
    'Case_burden': 'DALY / case',
})

metadata = {
    'notes': {
        'Population': 'Actual sum of the cities studied is 2085055 but here we ' +
        'assume that noise exposure only occurs in the cities listed, and smaller ' +
        'municipalities are free from transport noise. Thus, for Finland, we use ' +
        'the year 2020 value for population in Finland: 5530000.'
    },
    'references': {
        'General': 'http://fi.opasnet.org/fi/Liikenteen_terveysvaikutukset. For exposure data, ' +
        'see e.g. https://cdr.eionet.europa.eu/fi/eu/noise/df8/2017/envwjdfiq',
        'Er_function': 'All exposure-response functions: WHO & JRC 2011 (values scaled from % to ' +
        'fraction). https://apps.who.int/iris/handle/10665/326424',
        'Incidence': 'Nominal value that matches period and case burden',
        'Case_burden': 'disability weight 0.02, duration 1 year',
    }
}

ds_noise = Dataset(
    df=df, identifier='hia/exposure_response/noise',
    units=unit,
    metadata=metadata)


# Waterborne microbes

df = pd.DataFrame({
    'Erf_context': pd.Series([
        'campylobacter infection',
        'rotavirus infection',
        'norovirus infection',
        'sapovirus infection',
        'cryptosporidium infection',
        'E.coli O157:H7 infection',
        'giardia infection']),
    'Pollutant': pd.Series(['campylobacter', 'rotavirus', 'norovirus', 'sapovirus',
                            'cryptosporidium', 'E.coli O157:H7', 'giardia']),
    'Response': pd.Series(['infection'] * 7),
    'Period': pd.Series([1.] * 7),
    'Route': pd.Series(['ingestion'] * 7),
    'Er_function': pd.Series(['beta poisson approximation'] + ['exact beta poisson'] * 5 + ['exponential']),
    'Case_burden': pd.Series([0.002] * 7),
    'ingestion_p0_2': pd.Series([None, 0.167, 0.04, 0.04, 0.115, 0.157, None]),
    'ingestion_p0': pd.Series([0.024, 0.191, 0.055, 0.055, 0.176, 9.16, None]),
    'ingestion_p1': pd.Series([0.011] + [None] * 6),
    'ingestion_m1': pd.Series([None] * 6 + [0.0199]),
})

units = {
    'Period': 'days / incident',
    'Case_burden': 'DALY / case',
    'ingestion_p1': 'microbes / day',
    'ingestion_m1': 'day / microbes'
}

metadata = {
    'references': {
        'General': 'http://en.opasnet.org/w/Water_guide',
        'Er_function': 'http://en.opasnet.org/w/ERF_of_waterborne_microbes',
        'Case_burden': 'nominal value for gastroenteritis. Excludes complications http://en.opasnet.org/w/Case_burden_of_waterborne_microbes'
    }
}

ds_micr = Dataset(
    df=df,
    identifier='hia/exposure_response/microbes',
    units=units,
    metadata=metadata)


# Intake fractions

df = pd.DataFrame({
    'Pollutant': pd.Series(['PM10-2.5'] * 4 + ['PM2.5'] * 4 + ['SO2'] * 4 + ['NOx'] * 4 + ['NH3'] * 4),
    'Emission_height': pd.Series(['high', 'low', 'ground', 'average'] * 5),
    'urban': pd.Series([8.8, 13, 40, 37, 11, 15, 44, 26] + [0.99] * 4 + [0.2] * 4 + [1.7] * 4),
    'rural': pd.Series([0.7, 1.1, 3.7, 3.4, 1.6, 2, 3.8, 2.6] + [0.79] * 4 + [0.17] * 4 + [1.7] * 4),
    'remote': pd.Series([0.04, 0.04, 0.04, 0.04, 0.1, 0.1, 0.1, 0.1] + [0.05] * 4 + [0.01] * 4 + [0.1] * 4),
    'average': pd.Series([5, 7.5, 23, 21, 6.8, 6.8, 25, 15] + [0.89] * 4 + [0.18] * 4 + [1.7] * 4),
})

df = df.melt(id_vars=['Pollutant', 'Emission_height'], value_name='Value', var_name='Population_density')

metadata = {
    'references': {
        'General': 'Humbert et al. 2011 https://doi.org/10.1021/es103563z ' +
        'http://en.opasnet.org/w/Intake_fractions_of_PM#Data',
        'Emission_height': 'The values for SO2, NOx, and NH3 apply to all emission heights.'
    }
}

unit = dict({
    'Value': 'ppm'
})

ds_if = Dataset(
    df=df,
    identifier='hia/intake_fraction/air_pollution',
    units=unit,
    metadata=metadata)


# Toxic equivalency factors TEF

df = pd.DataFrame({
    'group': pd.Series(
        ['chlorinated dibenzo-p-dioxins'] * 7 +
        ['chlorinatd dibenzofurans'] * 10 +
        ['non-ortho-substituted PCBs'] * 4 +
        ['mono-ortho-substituted PCBs'] * 8),
    'compound': pd.Series([
        '2378-TCDD', '12378-PeCDD', '123478-HxCDD', '123678-HxCDD', '123789-HxCDD', '1234678-HpCDD', 'OCDD',
        '2378-TCDF', '12378-PeCDF', '23478-PeCDF', '123478-HxCDF', '123678-HxCDF', '123789-HxCDF', 
        '234678-HxCDF', '1234678-HpCDF', '1234789-HpCDF', 'OCDF', "3,3',4,4'-tetraCB", "3,4,4',5-tetraCB", 
        "3,3',4,4',5-pentaCB", "3,3',4,4',5,5'-hexaCB", "2,3,3',4,4'-pentaCB", "2,3,4,4',5-pentaCB", 
        "2,3',4,4',5-pentaCB", "2',3,4,4',5-pentaCB", "2,3,3',4,4',5-hexaCB", "2,3,3',4,4',5'-hexaCB", 
        "2,3',4,4',5,5'-hexaCB", "2,3,3',4,4',5,5'-heptaCB"]),
    'compound2': pd.Series([
        '2378TCDD', '12378PeCDD', '123478HxCDD', '123678HxCDD', '123789HxCDD', '1234678HpCDD',
        'OCDD', '2378TCDF', '12378PeCDF', '23478PeCDF', '123478HxCDF', '123678HxCDF',
        '123789HxCDF', '234678HxCDF', '1234678HpCDF', '1234789HpCDF', 'OCDF',
        '33_44_tetraCB', '344_5tetraCB', '33_44_5pentaCB', '33_44_55_hexaCB', '233_44_pentaCB',
        '2344_5pentaCB', '23_44_5pentaCB', '2_344_5pentaCB', '233_44_5hexaCB', 
        '233_44_5_hexaCB', '23_44_55_hexaCB', '233_44_55_heptaCB']),
    'compound3': pd.Series([
        'TCDD', 'PeCDD', '123478HCDD', '123678HCDD', '123789HCDD', None, None, 'TCDF', None, None,
        '123478HCDF', '123678HCDF', '123789HCDF', '234678HCDF', None, None, None,
        'PCB77', 'PCB81', 'PCB126', 'PCB169', 'PCB105', 'PCB114', 'PCB118', 'PCB123', 'PCB156', 'PCB157',
        'PCB167', 'PCB189'
    ]),
    'compound4': pd.Series([None] * 17 + ['CoPCB-77', 'CoPCB-81', 'CoPCB-126', 'CoPCB-169'] + [None] * 8),
    'Default_paradigm': pd.Series([False] * 29 + [True] * 29 + [False] * 29),
    'WHO1998': pd.Series([1, 1, 0.1, 0.1, 0.1, 0.01, 0.0001, 0.1, 0.05, 0.5, 0.1, 0.1, 0.1, 0.1,
        0.01, 0.01, 0.0001, 0.0001, 0.0001, 0.1, 0.01, 0.0001, 0.0005, 0.0001, 0.0001, 0.0005,
        0.0005, 0.00001, 0.0001]),
    'WHO2005': pd.Series([1, 1, 0.1, 0.1, 0.1, 0.01, 0.0003, 0.1, 0.03, 0.3, 0.1, 0.1, 0.1, 0.1,
        0.01, 0.01, 0.0003, 0.0001, 0.0003, 0.1, 0.03, 0.00003, 0.00003, 0.00003, 0.00003, 0.00003,
        0.00003, 0.00003, 0.00003]),
    'EU_IED2014': pd.Series([1, 0.5, 0.1, 0.1, 0.1, 0.01, 0.001, 0.1, 0.05, 0.5, 0.1, 0.1, 0.1,
        0.1, 0.01, 0.01, 0.001])
})

out = df.copy()[['group', 'compound', 'WHO1998', 'WHO2005', 'EU_IED2014']]
out = out.melt(id_vars=['group', 'compound'], var_name='paradigm', value_name='Value')
tmp = df.copy()[['group', 'compound2', 'WHO1998', 'WHO2005', 'EU_IED2014']]
tmp.rename(columns={'compound2': 'compound'}, inplace=True)
tmp = tmp.melt(id_vars=['group', 'compound'], var_name='paradigm', value_name='Value')
out = out.append(tmp)
tmp = df.copy()[['group', 'compound3', 'WHO1998', 'WHO2005', 'EU_IED2014']]
tmp.rename(columns={'compound3': 'compound'}, inplace=True)
tmp = tmp.melt(id_vars=['group', 'compound'], var_name='paradigm', value_name='Value')
out = out.append(tmp)
tmp = df.copy()[['group', 'compound4', 'WHO1998', 'WHO2005', 'EU_IED2014']]
tmp.rename(columns={'compound4': 'compound'}, inplace=True)
tmp = tmp.melt(id_vars=['group', 'compound'], var_name='paradigm', value_name='Value')
out = out.append(tmp)
out = out.dropna(how='any')

unit = {'Value': 'TEF'}

metadata = {
    'references': {
        'General': 'http://en.opasnet.org/w/Toxic_equivalency_factor http://en.opasnet.org/w/Toxic_equivalency_factor_references',
        'WHO2005': 'Martin Van den Berg, Linda S. Birnbaum, Michael Denison, Mike De Vito, ' +
            'William Farland, Mark Feeley, Heidelore Fiedler, Helen Hakansson, Annika Hanberg, ' +
            'Laurie Haws, Martin Rose, Stephen Safe, Dieter Schrenk, Chiharu Tohyama, Angelika ' +
            'Tritscher, Jouko Tuomisto, Mats Tysklind, Nigel Walker, and Richard E. Peterson: ' +
            'The 2005 World Health Organization Reevaluation of Human and Mammalian Toxic Equivalency ' +
            'Factors for Dioxins and Dioxin-Like Compounds. Toxicological Sciences 93(2), 223–241 ' +
            '(2006) doi:10.1093/toxsci/kfl055.'
    }
}

ds_tef = Dataset(
    df=out,
    identifier='hia/dioxin/toxic_equivalency_factors',
    units=unit,
    metadata=metadata
)

# Pollutants in food

df = pd.DataFrame({
    'Erf_context': pd.Series([
        'dioxin cancer', 'dioxin tolerable_weekly_intake', 'omega3 chd_mortality', 'omega3 breast_cancer',
        'vitamin_D deficiency']),
    'Period': pd.Series([80., 1, 80, 80, 1], dtype='pint[a / incident]'),
    'Route': pd.Series(['exposure'] * 2 + ['ingestion'] * 3),
    'Er_function': pd.Series(['unit risk', 'step function', 'relative Hill', 'relative risk', 'step function']),
    'exposure_m1': pd.Series([0.001, None, None, None, None], dtype='pint[kg d / pg]'),
    'exposure_p1': pd.Series([0., 0, None, None, None], dtype='pint[pg/kg/d]'),
    'exposure_p1_2': pd.Series([None, 2., None, None, None], dtype='pint[pg/kg/week]'),
    'ingestion_p1': pd.Series([None, None, 47, 0, 10e-3], dtype='pint[mg/d]'),
    'ingestion_p1_2': pd.Series([None, None, None, None, 100.], dtype='pint[ug/d]'),
    'ingestion_p0': pd.Series([None, None, None, 0., None]),
    'ingestion_m1': pd.Series([None, None, None, -0.5129329439, None], dtype='pint[d/g]'),
    'ingestion_p0': pd.Series([None, None, -0.17, None, None]),
    'Case_burden': pd.Series([19.7, 0.0001, 10, 19.7, 0.001], dtype='pint[DALY/case]'),
})

metadata = {
    'references': {
        'dioxin cancer': {
            'General': 'http://en.opasnet.org/w/ERF_of_dioxin',
            'Er_function': 'U.S.EPA 2004. https://cfpub.epa.gov/ncea/risk/recordisplay.cfm?deid=87843',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'dioxin tolerable_weekly_intake': {
            'General': 'http://en.opasnet.org/w/ERF_of_dioxin',
            'Er_function': 'EFSA dioxin recommendation 2018',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'omega3 chd_mortality': {
            'General': 'http://en.opasnet.org/w/ERF_of_omega-3_fatty_acids',
            'Er_function': 'Cohen et al 2005 http://www.ncbi.nlm.nih.gov/pubmed/16242602',
            'Case_burden': 'http://fi.opasnet.org/fi/Goherr_assessment#Model_parameters',
        },
        'omega3 breast_cancer': {
            'General': 'http://en.opasnet.org/w/ERF_of_omega-3_fatty_acids',
            'Er_function': 'log(0.95)/(0.1 g/d)',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'vitamin_D deficiency': {
            'Period': 'sunlight prevents deficiency during summer',
            'Er_function': 'http://en.opasnet.org/w/ERFs_of_vitamins',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
    }
}


ds_food = Dataset(
    df=df,
    identifier='hia/exposure_response/food',
    metadata=metadata
)


## Incidence

df = pd.DataFrame({
    'Erf_context': pd.Series([
        'dioxin cancer',
        'dioxin tolerable_weekly_intake',
        'omega3 chd_mortality',
        'omega3 breast_cancer',
        'vitamin_D deficiency',
        'PM2.5 mortality',
        'PM2.5 work_days_lost',
        'NOx mortality',
        'PM10 chronic_bronchitis',
        'campylobacter infection',
        'rotavirus infection',
        'norovirus infection',
        'sapovirus infection',
        'cryptosporidium infection',
        'E.coli O157:H7 infection',
        'giardia infection',
        'walking mortality',
        'cycling mortality',
     ]),
    'Place': pd.Series(['default'] * 18),
    'Population': pd.Series(['default'] * 18),
    'Incidence': pd.Series([0.002927583, 1, 0.0033423729, 93.58e-5, 1, 1363.8e-5, 12, 1363.8e-5, 390e-5] + [1] * 7 + [1363.8e-5] * 2, dtype='pint[cases/personyear]'),
})

metadata = {
    'references': {
        'dioxin cancer': {
            'Incidence': 'https://stat.fi/til/ksyyt/2020/ksyyt_2020_2021-12-10_tau_001_fi.html',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'dioxin tolerable_weekly_intake': {
            'Incidence': 'nominal; everyone is exposed',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'omega3 chd_mortality': {
            'Incidence': 'https://stat.fi/til/ksyyt/2020/ksyyt_2020_2021-12-10_tau_001_fi.html',
            'Case_burden': 'http://fi.opasnet.org/fi/Goherr_assessment#Model_parameters',
        },
        'omega3 breast_cancer': {
            'Incidence': 'https://syoparekisteri.fi/tilastot/tautitilastot/?_inputs_&value_type="inc.rate"&submit=2&tabset_panel="2"&value_theme="theme_inc"&tabu="inc_ts1"&table_cells_selected=[]&language="fi"&in.subset.sites=["0L","24L"]&in.subset.sex="-1L"&in.subset.area="-1L"&table_view="v2"&table_rows_selected=null&table_columns_selected=null&',
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'vitamin_D deficiency': {
            'Case_burden': 'http://en.opasnet.org/w/Goherr_assessment#Model_parameters',
        },
        'PM2.5 mortality': {
            'Incidence': 'http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_burden': 'De Leeuw & Horàlek 2016/5 http://fi.opasnet.org/fi/Kiltova#PAQ2018'
        },
        'PM2.5 work_days_lost': {
            'Incidence': 'http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_burden': '0.099 DW * 0.00274 a, Heimtsa & Intarese http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_cost': 'Holland et al., 2014'
        },
        'NOx mortality': {
            'Incidence': 'http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_burden': 'Same as PM2.5 De Leeuw & Horàlek 2016/5 http://fi.opasnet.org/fi/Kiltova#PAQ2018'
        },
        'PM10 chronic_bronchitis': {
            'Incidence': 'HRAPIE: SAPALDIA http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_burden': 'http://fi.opasnet.org/fi/Kiltova#PAQ2018',
            'Case_cost': 'Holland et al., 2014'
        },
        'Microbes': {
            'General':'Nominal value 1 case per personyear'
        },
    }
}

ds_incidence = Dataset(
    df=df,
    identifier='hia/incidence/default',
    metadata=metadata)

# Traffic emissions and emission factors

df = pd.read_csv('data/ALas_liikenne.csv')

unit = {
    'Emissions': 'kilotonnes',
    'Energy_consumption': 'GWh',
    'Mileage': 'Gm',
    'Emission_factor_e': 'g / Wh',
    'Emission_factor_m': 'g / m',
    'Mileage_factor': 'Wh / m'
}

metadata = {
    'references': {
        'General': 'Data comes from the ALas model developed by the Finnish Environment Institute. Data loaded in January 2022. https://hiilineutraalisuomi.fi/fi-FI/Paastot_ja_indikaattorit/Kuntien_ja_alueiden_kasvihuonekaasupaastot',
    },
    'notes': {
        'General': 'This data contains road transportation data only. It is a part of data /syke/alas_emissions but has more details.',
        'Emissions': 'Emissions are kilotonnes CO2-equivalent.',
    },
}

ds_traffic = Dataset(
    df=df,
    identifier='syke/alas_traffic',
    units=unit,
    metadata=metadata
)

#### European emission factor database for air pollution

df = pd.read_csv('data/efdb_eea_2019.csv')

metadata = {
    'notes': {
        'General': 'The full EFDB dataset contains 13209 entries, of which ' +
        '2841 are road transport data: 152 fuel consumptions and ' +
        '2689 emission factors. Of the emission factors, 202 were ' +
        'given per kg fuel used and 348 had units g/vehicle/d ' +
        '(NMVOC from fuel evaporation); these 550 entries were excluded. ' +
        'This left us with 19 emission factors about ' +
        'road abrasion and tyre and break ware, and 2120 emission factors ' +
        'about tailpipe emissions, i.e. 2139 emission factors that are ' +
        'in this dataset. All are in units g/km.',
        'Typical vehicles': {
            'Personal gasoline car': 'Petrol Medium - Euro 5 – EC 715/2007',
            'Personal diesel car': 'Diesel Medium - Euro 5 – EC 715/2007',
            'Bus': 'Urban Buses Standard - Euro V - 2008',
            'Motorbikes and mopeds': '4-stroke 250 - 750 cm³ - Conventional',
            'Heavy-duty vehicles': 'Diesel 16 - 32 t - Euro IV - 2005',
            'Light-duty vehicles': 'Diesel - Euro 5 – EC 715/2007',
        },
    },
    'references': {
        'General': 'EMEP/EEA air pollutant emission inventory guidebook 2019. http://efdb.apps.eea.europa.eu/'
    }
}
df.Value.loc[df.Value == 'na'] = None
df.Value.loc[df.Value == 'NC'] = None

unit = {
    'Value': 'g / km',
    'CI_lower': 'g / km',
    'CI_upper': 'g / km'
}

ds_ef = Dataset(
    df= df,
    identifier='eea/efdb',
    units=unit,
    metadata=metadata
)

# Noise: exposed fraction and nominal exposures

df = pd.read_csv('data/Health assessment data - Noise exposure.csv')

metadata = {
    'references': {
        'General': 'http://fi.opasnet.org/fi/Liikenteen_terveysvaikutukset ' +
        'Lehtomäki, H., Karvosenoja, N., Paunu, V-V., Korhonen, A., Hänninen, O., ' +
        'Tuomisto, J., Karppinen, A., Kukkonen, J. & Tainio, M. 2021. Liikenteen ' +
        'terveysvaikutukset Suomessa ja suurimmissa kaupungeissa. Suomen ympäristökeskuksen ' +
        'raportteja 16/2021. http://hdl.handle.net/10138/329273',
    }
}

ds_noise_fraction = Dataset(
    df=df,
    identifier='hia/exposure/noise_finland',
    metadata=metadata
)

df = pd.DataFrame({
    'Exposure level': pd.Series(['50-54', '55-59', '60-64', '65-69', '70-74', '>70', '>75']),
    'Value': pd.Series([52, 57, 62, 67, 72, 72, 77.5], dtype='pint[Lden]')
})

metadata = {
    'notes': 'These are nominal exposures of different noise level groups. The actual ' +
    'exposure is given as fraction of population (frexposed) that belongs to each group. ' +
    'Note that the unit is Lden although this is used for Lnight exposures as well. ' +
    'The actual noise exposure unit must be derived from the context.'
}

ds_noise_nominal = Dataset(
    df=df,
    identifier='hia/exposure/noise_nominal',
    metadata=metadata
)

### Water microbe concentrations

df = pd.read_csv('data/Health assessment data - Water microbes.csv')

unit = {
    'Value': 'microbes/l'
}

metadata = {
    'references': {
        'General': 'http://en.opasnet.org/w/Water_guide'
    }
}

ds_microbe_conc = Dataset(
    df=df,
    identifier='hia/concentration/water_microbes',
    units=unit,
    metadata=metadata
)

if False:
    repo.push_dataset(ds_noise)
    repo.push_dataset(ds_tef)
    repo.push_dataset(ds_traffic)
    repo.push_dataset(ds_ef)
    repo.push_dataset(ds_if)
    repo.push_dataset(ds_noise_nominal)
    repo.push_dataset(ds_noise_fraction)
    repo.push_dataset(ds_noise)
    repo.push_dataset(ds_microbe_conc)
    repo.push_dataset(ds_micr)
    repo.push_dataset(ds_phys_exposure)
    repo.push_dataset(ds_incidence)

repo.push_dataset(ds_act)
repo.push_dataset(ds_air)
repo.push_dataset(ds_indoor)
repo.push_dataset(ds_food)
