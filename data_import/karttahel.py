import io
import geopandas as gpd
import pandas as pd
from owslib.wfs import WebFeatureService


def read_wfs_layer():
    wfs = WebFeatureService(url='https://kartta.hel.fi/ws/geoserver/avoindata/wfs', version='2.0.0')
    out = wfs.getfeature(typename='avoindata:Rakennukset_alue_rekisteritiedot')

    s = out.read()
    s = s.encode('utf8')
    bio = io.BytesIO(s)

    df = gpd.read_file(bio)
    df = df.drop(columns='geometry')
    # df['geometry'] = df.geometry.astype(str)
    return df


BUILDING_CODES = {
    'ra_julkisivumat': {
        '1': 'Betoni',
        '2': 'Tiili',
        '3': 'Metallilevy',
        '4': 'Kivi',
        '5': 'Puu',
        '6': 'Lasi',
        '7': 'Muu'
    },
    'ra_kantrakaine': {
        '1': 'Betoni tai kevytbetoni',
        '2': 'Tiili',
        '3': 'Teräs',
        '4': 'Puu',
        '5': 'Muu'
    },
    'ra_kayttark': {
        '11': 'Yhden asunnon talot',
        '12': 'Kahden asunnon talot',
        '13': 'Muut erilliset pientalot',
        '21': 'Rivitalot',
        '22': 'Ketjutalot',
        '32': 'Luhtitalot',
        '39': 'Muut kerrostalot',
        '41': 'Vapaa-ajan asunnot',
        '111': 'Myymälähallit',
        '112': 'Liike- ja tavaratalot, kauppakeskukset',
        '119': 'Myymälärakennukset',
        '121': 'Hotellit, motellit, matkustajakodit, kylpylähotellit',
        '123': 'Loma- lepo- ja virkistyskodit',
        '124': 'Vuokrattavat lomamökit ja osakkeet (liiketoiminnallisesti)',
        '129': 'Muut majoitusliikerakennukset',
        '131': 'Asuntolat, vanhusten palvelutalot, asuntolahotellit',
        '139': 'Muut majoitusrakennukset',
        '141': 'Ravintolat, ruokalat ja baarit',
        '151': 'Toimistorakennukset',
        '161': 'Rautatie- ja linja-autoasemat, lento- ja satamaterminaalit',
        '162': 'Kulkuneuvojen suoja- ja huoltorakennukset',
        '163': 'Pysäköintitalot',
        '164': 'Tietoliikenteen rakennukset',
        '169': 'Muut liikenteen rakennukset',
        '211': 'Keskussairaalat',
        '213': 'Muut sairaalat',
        '214': 'Terveyskeskukset',
        '215': 'Terveydenhoidon erityislaitokset (mm. kuntoutuslaitokset)',
        '219': 'Muut terveydenhoitorakennukset',
        '221': 'Vanhainkodit',
        '222': 'Lastenkodit, koulukodit',
        '223': 'Kehitysvammaisten hoitolaitokset',
        '229': 'Muut huoltolaitosrakennukset',
        '231': 'Lasten päiväkodit',
        '239': 'Muut sosiaalitoimen rakennukset',
        '241': 'Vankilat',
        '311': 'Teatterit, konsertti- ja kongressitalot, oopperat',
        '312': 'Elokuvateatterit',
        '322': 'Kirjastot',
        '323': 'Museot, taidegalleriat',
        '324': 'Näyttelyhallit',
        '331': 'Seurain-, nuoriso- yms. talot',
        '341': 'Kirkot, kappelit, luostarit, rukoushuoneet',
        '342': 'Seurakuntatalot',
        '349': 'Muut uskonnollisten yhteisöjen rakennukset',
        '351': 'Jäähallit',
        '352': 'Uimahallit',
        '353': 'Tennis-, squash- ja sulkapallohallit',
        '354': 'Monitoimi- ja muut urheiluhallit',
        '359': 'Muut urheilu- ja kuntoilurakennukset',
        '369': 'Muut kokoontumisrakennukset',
        '511': 'Peruskoulut, lukiot ja muut',
        '521': 'Ammatilliset oppilaitokset',
        '531': 'Korkeakoulurakennukset',
        '532': 'Tutkimuslaitosrakennukset',
        '541': 'Järjestöjen, liittojen, työnantajien yms. opetusrakennukset',
        '549': 'Muualla luokittelemattomat opetusrakennukset',
        '611': 'Voimalaitosrakennukset',
        '613': 'Yhdyskuntatekniikan rakennukset',
        '691': 'Teollisuushallit',
        '692': 'Teollisuus- ja pienteollisuustalot',
        '699': 'Muut teollisuuden tuotantorakennukset',
        '711': 'Teollisuusvarastot',
        '712': 'Kauppavarastot',
        '719': 'Muut varastorakennukset',
        '721': 'Paloasemat',
        '722': 'Väestönsuojat',
        '729': 'Muut palo- ja pelastustoimen rakennukset',
        '811': 'Navetat, sikalat, kanalat yms.',
        '819': 'Eläinsuojat, ravihevostallit, maneesit',
        '891': 'Viljankuivaamot ja viljan säilytysrakennukset, siilot',
        '892': 'Kasvihuoneet',
        '893': 'Turkistarhat',
        '899': 'Muut maa-, metsä- ja kalatalouden rakennukset',
        '931': 'Saunarakennukset',
        '941': 'Talousrakennukset',
        '999': 'Muut rakennukset'
    },
    'ra_lammitystapa': {
        '1': 'Vesikeskuslämmitys',
        '2': 'Ilmakeskuslämmitys',
        '3': 'Suora sähkölämmitys',
        '4': 'Uunilämmitys',
        '5': 'Ei kiinteää lämmityslaitetta'},
    'ra_lammonlahde': {
        '1': 'Kauko- tai aluelämpö',
        '2': 'Kevyt polttoöljy',
        '3': 'Raskas polttoöljy',
        '4': 'Sähkö',
        '5': 'Kaasu',
        '6': 'Kivihiili, koksi tms.',
        '7': 'Puu',
        '8': 'Turve',
        '9': 'Maalämpö tms.',
        '10': 'Muu'
    },
    'tyyppi': {
        '0': 'Ei käyttömerkintää kartalla',
        '1': 'Asuinrakennus',
        '2': 'Yleinen tai liikerakennus',
        '3': 'Teollisuusrakennus',
        '4': 'Talousrakennus',
        '5': 'Maanalainen tila',
        '6': 'Muu käyttötarkoitus'
    },
    'olotila': {
        '1': 'Purettu',
        '2': 'Voimassa'
    },
    'laatu': {
        '0': 'Määrittely tekemättä',
        '1': 'Määritelty luotettavasti',
        '2': 'Määritelty tulkinnanvaraisesti',
        '3': 'Vaikeasti selvitettävissä',
        '4': 'Ei ole selvitettävissä'
    },
    'vastaavuus': {
        '0': 'Geometriaan ei liity RATU-kohdetta',
        '1': '1/1 (1 geometria/1 RATU-kohde)',
        '2': 'n/1 (n geometriaa/1 RATU-kohde)',
        '3': '1/n (1 geometria/n RATU-kohdetta)',
        '4': 'ei määritelty'
    }
}

BUILDING_CODE_COLS = {
    'tila_koodi': 'olotila',
    'ratu_vastaavuus_koodi': 'vastaavuus',
    'ratu_laatu_koodi': 'laatu',
    'tyyppi_koodi': 'tyyppi',
    'c_rakeaine': 'ra_kantrakaine',
    'c_poltaine': 'ra_lammonlahde',
    'c_lammtapa': 'ra_lammitystapa',
    'c_kayttark': 'ra_kayttark',
    'c_julkisivu': 'ra_julkisivumat',
}

COLUMN_MAPS = {
    'ratu': 'Rakennustunnus',
    'vtj_prt': 'VTJPRT',
    'tyyppi': None,
    'tyyppi_koodi': 'Tyyppi',
    'tila': None,
    'tila_koodi': 'Tila',
    'ratu_vastaavuus': None,
    'ratu_laatu': None,
    'c_kayttark': 'Käyttötarkoitus',

    'i_raktilav': 'Rakennustilavuus',
    'i_kokala': 'Kokonaisala',
    'i_kerrosala': 'Kerrosala',
    'i_kerrlkm': 'Kerrostenlkm',
    'i_kellarala': 'Kellariala',
    'i_huoneistojen_lkm': 'Huoneistojenlkm',
    'd_ashuoala': 'Asuinhuoneistoala',
    'c_vtj_prt': None,
    'c_valmpvm': 'Valmistumispvm',
    'c_lammtapa': 'Lämmitystapa',
    'c_rakeaine': 'Rakennusaine',
    'c_poltaine': 'Polttoaine',
    'c_julkisivu': 'Julkisivu',

    'ratu_vastaavuus_koodi': 'RakennustunnusVastaavuus',
    'ratu_laatu_koodi': 'RakennustunnusLaatu',
}


def get_buildings():
    df = pd.read_parquet('buildings.pq')
    for col, type_name in BUILDING_CODE_COLS.items():
        df[col] = df[col].astype('Int64').astype(str).map(BUILDING_CODES[str(type_name)])
        df[col] = df[col].astype('category')

    for col in ('c_hissi', 'c_viemlii'):
        df[col] = df[col].astype(bool)

    dt = df['c_valmpvm']
    df.loc[dt > '2030', 'c_valmpvm'] = None
    df.loc[dt < '1700', 'c_valmpvm'] = None
    df['c_valmpvm'] = pd.to_datetime(df['c_valmpvm']).dt.tz_convert('Europe/Helsinki')
    df = df.drop(columns='gml_id')

    df = df.drop(columns=[key for key, val in COLUMN_MAPS.items() if val is None])

    df['Postinumero'] = df['postinumero'].map('00{:,.0f}'.format, na_action='ignore')
    df = df.drop(columns=['postinumero', 'katunimi_ruotsi'])
    df['Osoite'] = df['katunimi_suomi'] + ' ' + df['osoitenumero']
    df = df.drop(columns=['katunimi_suomi', 'osoitenumero'])

    col_map = {key: val for key, val in COLUMN_MAPS.items() if val is not None}
    df = df.rename(columns=col_map)

    col_order = [x for x in COLUMN_MAPS.values() if x is not None]
    col_order.insert(2, 'Osoite')
    col_order.insert(3, 'Postinumero')

    rest = sorted(list(set(df.columns) - set(col_order)))

    df = df[col_order + rest]

    return df


def update_quilt_datasets():
    import quilt
    from quilt.data.jyrjola import karttahel

    df = get_buildings()
    karttahel._set(['buildings'], df)
    quilt.build('jyrjola/karttahel', karttahel)
    quilt.push('jyrjola/karttahel', is_public=True)


if __name__ == '__main__':
    #df = read_wfs_layer()
    #df.to_parquet('buildings.pq')
    update_quilt_datasets()
