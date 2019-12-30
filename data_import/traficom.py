import fastparquet
import pandas as pd
import numpy as np


def fetch_road_vehicle_register(fname, quarter):
    local_fname = 'data/traficom/%s' % fname
    try:
        with open(local_fname, 'r') as f:  # noqa
            url_or_fname = local_fname
    except Exception:
        url_or_fname = 'http://trafiopendata.97.fi/opendata/%s' % fname

    dtypes = {
        'ensirekisterointipvm': str,
        'variantti': str,
        'versio': str,
        'kayttoonottopvm': str,
        'mallimerkinta': str,
        'kaupallinenNimi': str,
        'tyyppihyvaksyntanro': str,
        'valmistenumero2': str,
        'jarnro': np.int32,
        'suurinNettoteho': np.float32,
    }
    bool_dtypes = ['ahdin', 'sahkohybridi']
    int8_dtypes = ['ovienLukumaara', 'istumapaikkojenLkm', 'sylintereidenLkm', 'vaihteidenLkm']
    int32_dtypes = [
        'omamassa', 'teknSuurSallKokmassa', 'tieliikSuurSallKokmassa', 'ajonKokPituus', 'ajonLeveys', 'ajonKorkeus', 'iskutilavuus',
        'Co2', 'matkamittarilukema',
    ]
    cat_dtypes = [
        'ajoneuvoluokka', 'ajoneuvoryhma', 'ajoneuvonkaytto', 'sahkohybridinluokka', 'korityyppi', 'ohjaamotyyppi', 'kayttovoima',
        'merkkiSelvakielinen', 'vaihteisto', 'voimanvalJaTehostamistapa', 'yksittaisKayttovoima', 'kunta',
    ]
    for col in int8_dtypes:
        dtypes[col] = np.float32
    for col in int32_dtypes:
        dtypes[col] = np.float32
    for col in cat_dtypes:
        dtypes[col] = 'category'
    for col in bool_dtypes:
        dtypes[col] = str

    print('Reading %s' % url_or_fname)
    if quarter > '2015':
        delimiter = ';'
    else:
        delimiter = ','
    df = pd.read_csv(url_or_fname, header=0, delimiter=delimiter, dtype=dtypes, encoding='iso8859-15', error_bad_lines=False)

    cat_map = {key: val[2] for key, val in KAYTTOVOIMA_CODES.items()}
    print('kayttovoima')
    df['kayttovoima'] = df['kayttovoima'].cat.rename_categories(cat_map)
    print('kunta')
    df['kunta'] = df['kunta'].cat.rename_categories(KUNTA_CODES)
    print('class')
    df['class'] = df['ajoneuvoluokka'].map(AJONEUVOLUOKKA_CODES).astype('category')

    print('kayttoonottopvm')
    df['kayttoonottopvm'] = df['kayttoonottopvm'].str.replace('0000', '1231')
    df['kayttoonottopvm'] = pd.to_datetime(df['kayttoonottopvm'], format='%Y%m%d', errors='coerce')
    print('ensirekisterointipvm')
    df['ensirekisterointipvm'] = pd.to_datetime(df['ensirekisterointipvm'], format='%Y-%m-%d')

    for col in int32_dtypes:
        print(col)
        df[col] = df[col].astype('Int32')

    for col in int8_dtypes:
        print(col)
        df[col] = df[col].where(df[col] < 127).round().astype('Int8')

    for col in bool_dtypes:
        print(col)
        df[col] = df[col].map(dict(true=1, false=0)).astype('Int8')

    object_encoding = {}

    for col, dtype in df.dtypes.iteritems():
        if dtype.name not in ('Int8', 'Int32'):
            object_encoding[col] = 'infer'
            continue
        print(col)
        object_encoding[col] = 'int32'
        df[col] = df[col].astype(object)

    print('writing')
    fastparquet.write('/tmp/out.pq', df, compression='snappy', object_encoding=object_encoding)


KAYTTOVOIMA_CODES = {
    '01': ('Bensiini', 'Bensin', 'Petrol'),
    '02': ('Dieselöljy', 'Dieselolja', 'Diesel fuel'),
    '03': ('Polttoöljy', 'Brännolja', 'Fuel oil'),
    '04': ('Sähkö', 'Elektricitet', 'Electricity'),
    '05': ('Vety', 'Hydrogen', 'Hydrogen'),
    '06': ('Kaasu', 'Gas', 'Gas'),
    '07': ('Metanoli', 'Metanol', 'Methanol'),
    '10': ('Biodiesel', 'Biodiesel', 'Biodiesel fuel'),
    '11': ('LPG', 'LPG', 'LPG'),
    '13': ('CNG', 'CNG', 'CNG'),
    '31': ('Moottoripetroli', 'Motorfotogen', 'Light fuel oil (kerosene)'),
    '32': ('Diesel/Puu', 'Diesel/Trä', 'Diesel/wood'),
    '33': ('Bensiini/Puu', 'Bensin/Trä', 'Petrol/wood'),
    '34': ('Bensiini + moottoripetroli', 'Bensin + motorfotogen', 'Petrol + light fuel oil (kerosene)'),
    '37': ('Etanoli', 'Etanol', 'Ethanol'),
    '38': ('Bensiini/CNG', 'Bensin/CNG', 'Petrol/CNG'),
    '39': ('Bensiini/Sähkö', 'Bensin/Elektricitet', 'Petrol/Electricity'),
    '40': ('Bensiini/Etanoli', 'Bensin/Etanol', 'Petrol/Ethanol'),
    '41': ('Bensiini/Metanoli', 'Bensin/Metanol', 'Petrol/Methanol'),
    '42': ('Bensiini/LPG', 'Bensin/LPG', 'Petrol/LPG'),
    '43': ('Diesel/CNG', 'Diesel/CNG', 'Diesel fuel / CNG'),
    '44': ('Diesel/Sähkö', 'Diesel/Elektricitet', 'Diesel fuel / Electricity'),
    '45': ('Diesel/Etanoli', 'Diesel/Etanol', 'Diesel fuel / Ethanol'),
    '46': ('Diesel/Metanoli', 'Diesel/Metanol', 'Diesel fuel / Methanol'),
    '47': ('Diesel/LPG', 'Diesel/LPG', 'Diesel fuel / LPG'),
    '48': ('Diesel/Biodiesel', 'Diesel/Biodiesel', 'Diesel fuel / Biodiesel fuel'),
    '49': ('Diesel/Biodiesel/Sähkö', 'Diesel/Biodiesel/Elektricitet', 'Diesel fuel / Biodiesel fuel / Electricity'),
    '50': ('Diesel/Biodiesel/Etanoli', 'Diesel/Biodiesel/Etanol', 'Diesel fuel / Biodiesel fuel / Ethanol'),
    '51': ('Diesel/Biodiesel/Metanoli', 'Diesel/Biodiesel/Metanol', 'Diesel fuel / Biodiesel fuel / Methanol'),
    '52': ('Diesel/Biodiesel/LPG', 'Diesel/Biodiesel/LPG', 'Diesel fuel / Biodiesel fuel / LPG'),
    '53': ('Diesel/Biodiesel/CNG', 'Diesel/Biodiesel/CNG', 'Diesel fuel / Biodiesel fuel / CNG'),
    '54': ('Vety/Sähkö', 'Hydrogen/Elektricitet', 'Hydrogen/Electricity'),
    '55': ('Dieselöljy/Muu', 'Dieselolja/Övrig', 'Diesel fuel / Other'),
    '56': ('H-ryhmän maakaasu', 'Naturgastyp H', 'Natural gas type H'),
    '57': ('L-ryhmän maakaasu', 'Naturgastyp L', 'Natural gas type L'),
    '58': ('HL-ryhmän maakaasu', 'Naturgastyp HL', 'Natural gas type HL'),
    '59': ('CNG/Biometaani', 'CNG/Biometan', 'Natural gas / Biomethane'),
    '60': ('Biometaani', 'Biometan', 'Biomethan'),
    '61': ('Puu', 'Träd', 'Wood'),
    '62': ('Etanoli (ED95)', 'Etanol (ED95)', 'Ethanol (ED95)'),
    '63': ('Etanoli (E85)', 'Etanol (E85)', 'Ethanol (E85)'),
    '64': ('Vety-maakaasuseos', 'H2NG-blandning', 'H2NG'),
    '65': ('LNG', 'LNG', 'LNG'),
    '66': ('LNG20', 'LNG20', 'LNG20'),
    '67': ('Diesel/LNG', 'Diesel/LNG', 'Diesel/LNG'),
    '68': ('Diesel/LNG20', 'Diesel/LNG20', 'Diesel/LNG20'),
    'X': ('Ei sovellettavissa', 'Ej tillämplig', 'Not applicable'),
    'Y': ('Muu', 'Övrig', 'Other'),
}


KUNTA_CODES = {
    '005': 'Alajärvi',
    '009': 'Alavieska',
    '010': 'Alavus',
    '016': 'Asikkala',
    '018': 'Askola',
    '019': 'Aura',
    '020': 'Akaa',
    '035': 'Brändö',
    '043': 'Eckerö',
    '046': 'Enonkoski',
    '047': 'Enontekiö',
    '049': 'Espoo',
    '050': 'Eura',
    '051': 'Eurajoki',
    '052': 'Evijärvi',
    '060': 'Finström',
    '061': 'Forssa',
    '062': 'Föglö',
    '065': 'Geta',
    '069': 'Haapajärvi',
    '071': 'Haapavesi',
    '072': 'Hailuoto',
    '074': 'Halsua',
    '075': 'Hamina',
    '076': 'Hammarland',
    '077': 'Hankasalmi',
    '078': 'Hanko',
    '079': 'Harjavalta',
    '081': 'Hartola',
    '082': 'Hattula',
    '086': 'Hausjärvi',
    '090': 'Heinävesi',
    '091': 'Helsinki',
    '092': 'Vantaa',
    '097': 'Hirvensalmi',
    '098': 'Hollola',
    '099': 'Honkajoki',
    '102': 'Huittinen',
    '103': 'Humppila',
    '105': 'Hyrynsalmi',
    '106': 'Hyvinkää',
    '108': 'Hämeenkyrö',
    '109': 'Hämeenlinna',
    '111': 'Heinola',
    '139': 'Ii',
    '140': 'Iisalmi',
    '142': 'Iitti',
    '143': 'Ikaalinen',
    '145': 'Ilmajoki',
    '146': 'Ilomantsi',
    '148': 'Inari',
    '149': 'Inkoo',
    '151': 'Isojoki',
    '152': 'Isokyrö',
    '153': 'Imatra',
    '165': 'Janakkala',
    '167': 'Joensuu',
    '169': 'Jokioinen',
    '170': 'Jomala',
    '171': 'Joroinen',
    '172': 'Joutsa',
    '176': 'Juuka',
    '177': 'Juupajoki',
    '178': 'Juva',
    '179': 'Jyväskylä',
    '181': 'Jämijärvi',
    '182': 'Jämsä',
    '186': 'Järvenpää',
    '198': 'Ulkomaat1',
    '199': 'Tuntematon',
    '200': 'Ulkomaat2',
    '201': 'Pohjoismaat',
    '202': 'Kaarina',
    '204': 'Kaavi',
    '205': 'Kajaani',
    '208': 'Kalajoki',
    '211': 'Kangasala',
    '213': 'Kangasniemi',
    '214': 'Kankaanpää',
    '216': 'Kannonkoski',
    '217': 'Kannus',
    '218': 'Karijoki',
    '224': 'Karkkila',
    '226': 'Karstula',
    '230': 'Karvia',
    '231': 'Kaskinen',
    '232': 'Kauhajoki',
    '233': 'Kauhava',
    '235': 'Kauniainen',
    '236': 'Kaustinen',
    '239': 'Keitele',
    '240': 'Kemi',
    '241': 'Keminmaa',
    '244': 'Kempele',
    '245': 'Kerava',
    '249': 'Keuruu',
    '250': 'Kihniö',
    '256': 'Kinnula',
    '257': 'Kirkkonummi',
    '260': 'Kitee',
    '261': 'Kittilä',
    '263': 'Kiuruvesi',
    '265': 'Kivijärvi',
    '271': 'Kokemäki',
    '272': 'Kokkola',
    '273': 'Kolari',
    '275': 'Konnevesi',
    '276': 'Kontiolahti',
    '280': 'Korsnäs',
    '284': 'Koski Tl',
    '285': 'Kotka',
    '286': 'Kouvola',
    '287': 'Kristiinankaupunki',
    '288': 'Kruunupyy',
    '290': 'Kuhmo',
    '291': 'Kuhmoinen',
    '295': 'Kumlinge',
    '297': 'Kuopio',
    '300': 'Kuortane',
    '301': 'Kurikka',
    '304': 'Kustavi',
    '305': 'Kuusamo',
    '309': 'Outokumpu',
    '312': 'Kyyjärvi',
    '316': 'Kärkölä',
    '317': 'Kärsämäki',
    '318': 'Kökar',
    '320': 'Kemijärvi',
    '322': 'Kemiönsaari',
    '398': 'Lahti',
    '399': 'Laihia',
    '400': 'Laitila',
    '402': 'Lapinlahti',
    '403': 'Lappajärvi',
    '405': 'Lappeenranta',
    '407': 'Lapinjärvi',
    '408': 'Lapua',
    '410': 'Laukaa',
    '416': 'Lemi',
    '417': 'Lemland',
    '418': 'Lempäälä',
    '420': 'Leppävirta',
    '421': 'Lestijärvi',
    '422': 'Lieksa',
    '423': 'Lieto',
    '425': 'Liminka',
    '426': 'Liperi',
    '430': 'Loimaa',
    '433': 'Loppi',
    '434': 'Loviisa',
    '435': 'Luhanka',
    '436': 'Lumijoki',
    '438': 'Lumparland',
    '440': 'Luoto',
    '441': 'Luumäki',
    '444': 'Lohja',
    '445': 'Parainen',
    '475': 'Maalahti',
    '478': 'Maarianhamina',
    '480': 'Marttila',
    '481': 'Masku',
    '483': 'Merijärvi',
    '484': 'Merikarvia',
    '489': 'Miehikkälä',
    '491': 'Mikkeli',
    '494': 'Muhos',
    '495': 'Multia',
    '498': 'Muonio',
    '499': 'Mustasaari',
    '500': 'Muurame',
    '503': 'Mynämäki',
    '504': 'Myrskylä',
    '505': 'Mäntsälä',
    '507': 'Mäntyharju',
    '508': 'Mänttä-Vilppula',
    '529': 'Naantali',
    '531': 'Nakkila',
    '535': 'Nivala',
    '536': 'Nokia',
    '538': 'Nousiainen',
    '541': 'Nurmes',
    '543': 'Nurmijärvi',
    '545': 'Närpiö',
    '560': 'Orimattila',
    '561': 'Oripää',
    '562': 'Orivesi',
    '563': 'Oulainen',
    '564': 'Oulu',
    '576': 'Padasjoki',
    '577': 'Paimio',
    '578': 'Paltamo',
    '580': 'Parikkala',
    '581': 'Parkano',
    '583': 'Pelkosenniemi',
    '584': 'Perho',
    '588': 'Pertunmaa',
    '592': 'Petäjävesi',
    '593': 'Pieksämäki',
    '595': 'Pielavesi',
    '598': 'Pietarsaari',
    '599': 'Pedersören kunta',
    '601': 'Pihtipudas',
    '604': 'Pirkkala',
    '607': 'Polvijärvi',
    '608': 'Pomarkku',
    '609': 'Pori',
    '611': 'Pornainen',
    '614': 'Posio',
    '615': 'Pudasjärvi',
    '616': 'Pukkila',
    '619': 'Punkalaidun',
    '620': 'Puolanka',
    '623': 'Puumala',
    '624': 'Pyhtää',
    '625': 'Pyhäjoki',
    '626': 'Pyhäjärvi',
    '630': 'Pyhäntä',
    '631': 'Pyhäranta',
    '635': 'Pälkäne',
    '636': 'Pöytyä',
    '638': 'Porvoo',
    '678': 'Raahe',
    '680': 'Raisio',
    '681': 'Rantasalmi',
    '683': 'Ranua',
    '684': 'Rauma',
    '686': 'Rautalampi',
    '687': 'Rautavaara',
    '689': 'Rautjärvi',
    '691': 'Reisjärvi',
    '694': 'Riihimäki',
    '697': 'Ristijärvi',
    '698': 'Rovaniemi',
    '700': 'Ruokolahti',
    '702': 'Ruovesi',
    '704': 'Rusko',
    '707': 'Rääkkylä',
    '710': 'Raasepori',
    '729': 'Saarijärvi',
    '732': 'Salla',
    '734': 'Salo',
    '736': 'Saltvik',
    '738': 'Sauvo',
    '739': 'Savitaipale',
    '740': 'Savonlinna',
    '742': 'Savukoski',
    '743': 'Seinäjoki',
    '746': 'Sievi',
    '747': 'Siikainen',
    '748': 'Siikajoki',
    '749': 'Siilinjärvi',
    '751': 'Simo',
    '753': 'Sipoo',
    '755': 'Siuntio',
    '758': 'Sodankylä',
    '759': 'Soini',
    '761': 'Somero',
    '762': 'Sonkajärvi',
    '765': 'Sotkamo',
    '766': 'Sottunga',
    '768': 'Sulkava',
    '771': 'Sund',
    '777': 'Suomussalmi',
    '778': 'Suonenjoki',
    '781': 'Sysmä',
    '783': 'Säkylä',
    '785': 'Vaala',
    '790': 'Sastamala',
    '791': 'Siikalatva',
    '831': 'Taipalsaari',
    '832': 'Taivalkoski',
    '833': 'Taivassalo',
    '834': 'Tammela',
    '837': 'Tampere',
    '844': 'Tervo',
    '845': 'Tervola',
    '846': 'Teuva',
    '848': 'Tohmajärvi',
    '849': 'Toholampi',
    '850': 'Toivakka',
    '851': 'Tornio',
    '853': 'Turku',
    '854': 'Pello',
    '857': 'Tuusniemi',
    '858': 'Tuusula',
    '859': 'Tyrnävä',
    '886': 'Ulvila',
    '887': 'Urjala',
    '889': 'Utajärvi',
    '890': 'Utsjoki',
    '892': 'Uurainen',
    '893': 'Uusikaarlepyy',
    '895': 'Uusikaupunki',
    '905': 'Vaasa',
    '908': 'Valkeakoski',
    '911': 'Valtimo',
    '915': 'Varkaus',
    '918': 'Vehmaa',
    '921': 'Vesanto',
    '922': 'Vesilahti',
    '924': 'Veteli',
    '925': 'Vieremä',
    '927': 'Vihti',
    '931': 'Viitasaari',
    '934': 'Vimpeli',
    '935': 'Virolahti',
    '936': 'Virrat',
    '941': 'Vårdö',
    '946': 'Vöyri',
    '976': 'Ylitornio',
    '977': 'Ylivieska',
    '980': 'Ylöjärvi',
    '981': 'Ypäjä',
    '989': 'Ähtäri',
    '992': 'Äänekoski',
    '999': 'Ei vak asuinkuntaa',
}


AJONEUVOLUOKKA_CODES = {
    'C1': 'Tractor',
    'C2': 'Tractor',
    'C3': 'Tractor',
    'C4': 'Tractor',
    'C5': 'Tractor',
    'KNP': 'Light quadricycle',
    'L1': 'Moped',
    'L1e': 'Moped',
    'L2': 'Moped',
    'L2e': 'Moped',
    'L3': 'Motorcycle',
    'L3e': 'Motorcycle',
    'L4': 'Motorcycle',
    'L4e': 'Motorcycle',
    'L5': 'Motor tricycle or quadricycle',
    'L5e': 'Motor tricycle',
    'L6e': 'Light quadricycle',
    'L7e': 'Quadricycle',
    'LTR': 'Tractor registered for road use',
    'M1': 'Car',
    'M1G': 'Car',
    'M2': 'Bus/coach',
    'M2G': 'Bus/coach',
    'M3': 'Bus/coach',
    'M3G': 'Bus/coach',
    'MA': 'All-terrain vehicle',
    'MTK': 'Public works vehicle',
    'N1': 'Van',
    'N1G': 'Van',
    'N2': 'Lorry',
    'N2G': 'Lorry',
    'N3': 'Lorry',
    'N3G': 'Lorry',
    'O1': 'Light trailer',
    'O2': 'Trailer',
    'O3': 'Trailer',
    'O4': 'Trailer',
    'Ra1': 'Tractor trailer',
    'Ra2': 'Tractor trailer',
    'Ra3': 'Tractor trailer',
    'Ra4': 'Tractor trailer',
    'Rb1': 'Tractor trailer',
    'Rb2': 'Tractor trailer',
    'Rb3': 'Tractor trailer',
    'Rb4': 'Tractor trailer',
    'Sa1': 'Towed machinery',
    'Sa2': 'Towed machinery',
    'Sb1': 'Towed machinery',
    'Sb2': 'Towed machinery',
    'T': 'Tractor',
    'T1': 'Tractor',
    'T2': 'Tractor',
    'T3': 'Tractor',
    'T4': 'Tractor',
    'T5': 'Tractor',
}


VEHICLE_URLS = [
    ('http://trafiopendata.97.fi/opendata/AvoinData31-1-2015-2.zip', '2014q4'),
    ('http://trafiopendata.97.fi/opendata/AvoinData20160101.zip', '2015q4'),
    ('http://trafiopendata.97.fi/opendata/20180425_Tieliikenne_5.2.zip', '2018q1'),
    ('http://trafiopendata.97.fi/opendata/10072018_Ajoneuvojen_avoin_data_5_3.zip', '2018q2'),
    # ('http://trafiopendata.97.fi/opendata/Tieliikenteen_avoindata_5.4.zip', '2018q3'),
    ('http://trafiopendata.97.fi/opendata/Tieliikenne_Avoin_Data_5.6.zip', '2019q1'),
    ('http://trafiopendata.97.fi/opendata/TieliikenneAvoinData_5_8.zip', '2019q3'),
]

if __name__ == '__main__':
    import quilt
    try:
        pass
        #quilt.install('jyrjola/traficom', force=True)
    except Exception:
        pass

    # quilt.push('jyrjola/traficom', is_public=True)
    from quilt.data.jyrjola import traficom  # noqa

    for url, quarter in VEHICLE_URLS:
        print(url)
        dataset_name = 'vehicle_register_%s' % quarter
        if dataset_name in traficom._keys():
            print('skipping')
            continue
        fetch_road_vehicle_register(url.split('/')[-1], quarter)
        print('build')
        quilt.build('jyrjola/traficom/%s' % dataset_name, '/tmp/out.pq')
        print('push')
        quilt.push('jyrjola/traficom', is_public=True)
