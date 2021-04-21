import json
from pprint import pprint

import pandas as pd
from gql import Client, gql
from gql.transport.requests import RequestsHTTPTransport
from pandas_pcaxis import PxParser

from aplans_graphs import post_values

transport = RequestsHTTPTransport(
    url='https://api.watch.kausal.tech/v1/graphql/', verify=True
)

client = Client(transport=transport, fetch_schema_from_transport=True)
GET_PLAN_INDICATORS = gql("""
    query getPlanIndicators($plan: ID!) {
        planIndicators(plan: $plan) {
            id
            name
            unit {
                name
            }
            quantity {
                name
            }
            latestValue {
                date
                value
            }
        }
    }
""")


INDICATORS = [{
    'name': 'Katu- ja ulkovalaistuksen energiankulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/4-kaupungin-omistamat/e40-ulkovalaistus.px',
    'px_topic': 'Ympäristötilastot/02_Energia/4_kaupungin_omistamat/E40_ulkovalaistus.px',
    'indicator_id': 93,
    'query': 'Muuttuja == "Kulutus (MWh/a)"',
    'unit': 'MWh/year',
}, {
    'name': 'Helsingin satama-alueiden CO2-päästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l018-laivaliikenne-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L018_laivaliikenne_paastot.px',
    'query': """Sektori == "Satamatoiminnat yhteensä" & Päästölaji in ['NOx (t)', 'CO (t)', 'CO2 (t)', 'HC (t)', 'SO2 (t)']""",
    'lambda_over': 'Päästölaji',
    'lambda': lambda x: x['CO2 (t)'] * 1.0,
    'indicator_id': 290,
    # FIXME: others are not GHGs?
}, {
    'name': 'Kaukolämmönkulutuksen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 31,
    'query': 'Alue == "Helsinki" & Sektori == "Kaukolämpö" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    'name': 'Rakennuskannan kaukolämmönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/1-energian-kulutus/e03-energian-kokonaiskulutus.px',
    'px_topic': 'Ympäristötilastot/02_Energia/1_Energian_kulutus/E03_energian_kokonaiskulutus.px',
    'indicator_id': 69,
    'query': 'Alue == "Helsinki" & Sektori == "Kaukolämpö" & Muuttuja == "Kokonaiskulutus (GWh)"',
    'unit': 'GWh/year'
}, {
    'name': 'Raskaan liikenteen kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'query': 'Alue == "Helsinki" & Ajoneuvoluokka in ["Kuorma-autot", "Linja-autot"] & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'sum_over': 'Ajoneuvoluokka',
    'indicator_id': 377,
}, {
    'name': 'henkilöautojen sähköajoneuvojen osuus henkilöautokannasta',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/3-autokanta/l26-autokanta-kayttovoimat.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/3_Autokanta/L26_autokanta_kayttovoimat.px',
    'query': 'Kunta == "Helsinki"',
    'indicator_id': 371,
    'lambda_over': 'Käyttövoima',
    # calc correct
    'lambda': lambda x: ((x['Bensiini/sähkö (ladattava hybridi)'] + x['Diesel/sähkö (ladattava)'] + x['Sähkö']) / x['Yhteensä']) * 100,
    # XXXXX: Problem in data
}, {
    'name': 'Joukkoliikenteen kulkumuoto-osuus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-2kulkutapajakauma/l27-matka-kulkutapa-pks-2000-2008.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_2Kulkutapajakauma/L27_matka_kulkutapa_PKS_2000_2008.px',
    'query': 'Alue == "Koko Helsinki" & Muuttuja == "Osuus (%)" & Kulkutapa == "Joukkoliikenne yhteensä"',
    'indicator_id': 197,
}, {
    'name': 'Liikenteen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'query': 'Alue == "Helsinki" & Ajoneuvoluokka == "Yhteensä" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'indicator_id': 24,
}, {
    'name': 'Jalankulun ja pyöräliikenteen kulkumuoto-osuus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-2kulkutapajakauma/l27-matka-kulkutapa-pks-2000-2008.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_2Kulkutapajakauma/L27_matka_kulkutapa_PKS_2000_2008.px',
    'indicator_id': 353,
    'query': 'Alue == "Koko Helsinki" & Muuttuja == "Osuus (%)"',
    'lambda_over': 'Kulkutapa',
    'lambda': lambda x: ((x['Kävely'] + x['Polkupyörä']) / x['Yht'] * 100)
}, {
    'name': 'Pyöräilyn kulkumuoto-osuus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-2kulkutapajakauma/l27-matka-kulkutapa-pks-2000-2008.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_2Kulkutapajakauma/L27_matka_kulkutapa_PKS_2000_2008.px',
    'query': 'Alue == "Koko Helsinki" & Muuttuja == "Osuus (%)"',
    'indicator_id': 175,
    'lambda_over': 'Kulkutapa',
    'lambda': lambda x: (x['Polkupyörä'] / x['Yht'] * 100)
}, {
    'name': 'Helsingin kaupungin kasvihuonekaasupäästöt (scope 1-2) (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 5,
    'query': 'Alue == "Helsinki" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'lambda_over': 'Sektori',
    'lambda': lambda x: x['Yhteensä'],
}, {
    'name': 'Vihreä Lippu -päiväkotien, peruskoulujen ja lukioiden määrä',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/6-pack/ymparistovastuullisuus/a6-vihrea-lippu.px',
    'px_topic': 'Ympäristötilastot/6_pack/Ympäristövastuullisuus/A6_vihrea_lippu.px',
    'indicator_id': 366,
    'query': 'Kaupunki == "Helsinki" & Muuttuja == "Mukana ohjelmassa (kpl)"',
}, {
    'name': 'Kaupunkilaisille suunnattujen ympäristökasvatustapahtumien määrä',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/6-pack/ymparistovastuullisuus/a3-kasvatus-osallistujat.px',
    'px_topic': 'Ympäristötilastot/6_pack/Ympäristövastuullisuus/A3_kasvatus_osallistujat.px',
    'indicator_id': 224,
    'query': 'Kaupunki == "Helsinki"',
    # FIXME -> Osuus väestöstä
    # Kaupungin järjestämään ympäristökasvatukseen osallistuminen kuudessa suurimmassa kaupungissa (% koko väestöstä)
    'skip': True,
}, {
    'name': 'Joukkoliikenteen matkojen määrä',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-liikennemaarat/l11-joukkoliikennematkat.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_Liikennemaarat/L11_joukkoliikennematkat.px',
    'indicator_id': 152,
    'query': 'Kunta == "Helsinki"',
    'lambda_over': 'Kulkutapa',
    'lambda': lambda x: (
        x['Raitioliikenne'] + x['Sisäinen bussiliikenne'] + \
        (0 if x['Kutsuplus'] == '..' else x['Kutsuplus']) + \
        x['Metroliikenne'] + x['Suomenlinnan lautta']
    )
}, {
    'name': 'Pyöräväylien pituus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/6-pack/liikenne/l3-pyoratieverkko.px',
    'px_topic': 'Ympäristötilastot/6_pack/Liikenne/L3_pyoratieverkko.px',
    'indicator_id': 1,
    'query': 'Kunta == "Helsinki" & Muuttuja == "Kaikki yhteensä (km)"',
}, {
    'name': 'Lämpimän käyttöveden kulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/07-vedet/2-vedenkulutus-vesihuolto/2-vedenkulutus-vesihuolto/v11-vedenkulutus-kuluttajaryhma.px',
    'px_topic': 'Ympäristötilastot/07_Vedet/2_Vedenkulutus_vesihuolto/2_Vedenkulutus_vesihuolto/v11_vedenkulutus_kuluttajaryhma.px',
    'query': 'Kuluttajaryhmä == "Yhteensä"',
    'indicator_id': 295,
}, {
    'name': 'Ympäristökasvatustapahtumiin ja ilmasto- ja energianeuvontaan osallistuneiden määrä',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/6-pack/ymparistovastuullisuus/a3-kasvatus-osallistujat.px',
    'px_topic': 'Ympäristötilastot/6_pack/Ympäristövastuullisuus/A3_kasvatus_osallistujat.px',
    'query': 'Kaupunki == "Helsinki"',
    'indicator_id': 223,
    # FIXME -> Osuus väestöstä
    # Kaupungin järjestämään ympäristökasvatukseen osallistuminen kuudessa suurimmassa kaupungissa (% koko väestöstä)
}, {
    'name': 'Öljylämmitteisten kiinteistöjen pinta-ala',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/2-rakennuskanta/e25-lammitysmuodot-uudisrakennustyyppi.px',
    'px_topic': 'Ympäristötilastot/02_Energia/2_Rakennuskanta/E25_lammitysmuodot_uudisrakennustyyppi.px',
    'indicator_id': 40,
    # FIXME: Wrong statistics
    'skip': True,
}, {
    'name': 'Kaupungin omistamien kaukolämmitteisten kiinteistöjen ominaislämmönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/4-kaupungin-omistamat/e21-kaup-rakennus-tyypeittain.px',
    'px_topic': 'Ympäristötilastot/02_Energia/4_kaupungin_omistamat/E21_kaup_rakennus_tyypeittain.px',
    'indicator_id': 4,
    'query': 'Rakennustyyppi == "Kaikki yhteensä" & Muuttuja == "Sääkorjattu lämpö (kWh/m2)"'
}, {
    'name': 'Kulutussähkönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/1-energian-kulutus/e03-energian-kokonaiskulutus.px',
    'px_topic': 'Ympäristötilastot/02_Energia/1_Energian_kulutus/E03_energian_kokonaiskulutus.px',
    'query': 'Alue == "Helsinki" & Sektori == "Kulutussähkö" & Muuttuja == "Kokonaiskulutus (GWh)"',
    'indicator_id': 92,
}, {
    'name': 'Sähkölämmityksen sähkönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/1-energian-kulutus/e03-energian-kokonaiskulutus.px',
    'px_topic': 'Ympäristötilastot/02_Energia/1_Energian_kulutus/E03_energian_kokonaiskulutus.px',
    'query': 'Alue == "Helsinki" & Sektori == "Sähkölämmitys" & Muuttuja == "Kokonaiskulutus (GWh)"',
    'indicator_id': 282,
}, {
    'name': 'Rakennuskannan kaukolämmönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/1-energian-kulutus/e03-energian-kokonaiskulutus.px',
    'px_topic': 'Ympäristötilastot/02_Energia/1_Energian_kulutus/E03_energian_kokonaiskulutus.px',
    'query': 'Alue == "Helsinki" & Sektori == "Kaukolämpö" & Muuttuja == "Kokonaiskulutus (GWh)"',
    'indicator_id': 69,
}, {
    'name': 'Kaupungin omistamien kiinteistöjen kaukolämmönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/4-kaupungin-omistamat/e21-kaup-rakennus-tyypeittain.px',
    'px_topic': 'Ympäristötilastot/02_Energia/4_kaupungin_omistamat/E21_kaup_rakennus_tyypeittain.px',
    'query': 'Rakennustyyppi == "Kaikki yhteensä" & Muuttuja == "Lämpö (GWh)"',
    'indicator_id': 102,
}, {
    'name': 'Kävelyn kulkumuoto-osuus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-2kulkutapajakauma/l27-matka-kulkutapa-pks-2000-2008.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_2Kulkutapajakauma/L27_matka_kulkutapa_PKS_2000_2008.px',
    'indicator_id': 113,
    'query': 'Alue == "Koko Helsinki" & Muuttuja == "Osuus (%)"',
    'lambda_over': 'Kulkutapa',
    'lambda': lambda x: ((x['Kävely']) / x['Yht'] * 100)
}, {
    'name': 'Kestävien kulkumuotojen osuus tehdyistä matkoista',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-2kulkutapajakauma/l27-matka-kulkutapa-pks-2000-2008.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_2Kulkutapajakauma/L27_matka_kulkutapa_PKS_2000_2008.px',
    'indicator_id': 2,
    'query': 'Alue == "Koko Helsinki" & Muuttuja == "Osuus (%)"',
    'lambda_over': 'Kulkutapa',
    'lambda': lambda x: ((x['Kävely'] + x['Polkupyörä'] + x['Joukkoliikenne yhteensä']) / x['Yht'] * 100)
}, {
    # Tilastossa (ei löydy rajapinnasta)Taulukko: Helsingin kaupunkisuunnittelulautakunnan puoltamien asemakaavojen sijoittuminen eri vyöhykkeisiin vuodesta 2008
    'name': 'Kerrosala, joka on kaavoitettu raideliikenteen lähelle',
    'indicator_id': 52,
    'skip': True,
    # FIXME: Katsotaan myöhemmin
}, {
    'name': 'Helen Oy:n sähköntuotannon kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/e7-energiantuotanto-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/E7_energiantuotanto_paastot.px',
    'indicator_id': 81,
    # FIXME: ---> Yhdistetään yhdeksi mittariksi: energiantuotanto
    'query': 'Energialaitos == "Helen" & Päästö == "CO2 (1000 t/a)"',
}, {
    'name': 'Helen Oy:n kaukolämmöntuotannon kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/e7-energiantuotanto-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/E7_energiantuotanto_paastot.px',
    'indicator_id': 82,
    'skip': True,
    # FIXME: Ks. yllä
}, {
    'name': 'Maatalouden kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 284,
    'query': 'Alue == "Helsinki" & Sektori == "Maatalous" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    'name': 'Sähkölämmityksen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 104,
    'query': 'Alue == "Helsinki" & Sektori == "Sähkölämmitys" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    'name': 'Lämmönkulutuksen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 25,
    'query': 'Alue == "Helsinki" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'lambda_over': 'Sektori',
    'lambda': lambda x: x['Kaukolämpö'] + x['Öljylämmitys'] + x['Sähkölämmitys'],
}, {
    'name': 'Teollisuuden ja työkoneiden kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 66,
    'query': 'Alue == "Helsinki" & Sektori == "Teollisuus ja työkoneet" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    'name': 'Jätteidenkäsittelyn kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 283,
    'query': 'Alue == "Helsinki" & Sektori == "Jätteiden käsittely" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    # Ympäristötilastossa (ei rajapinnassa): Taulukko: Helsingin vähäpäästöiset alle 100 g CO2/km autot vuodesta 200
    'name': 'Vähäpäästöisten ajoneuvojen tunnusten osuus asukas- ja yrityspysäköintitunnuksista',
    'indicator_id': 229,
    'skip': True,
    # FIXME: Vuodenvaihteen jälkeen
}, {
    'name': 'Henkilöautoilun kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 11,
    'query': 'Alue == "Helsinki" & Ajoneuvoluokka == "Henkilöautot" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"'
}, {
    'name': 'Polttomoottorihenkilöautoilla ajetut kilometrit',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/05-liikenne/2-liikennemaarat/l7-suorite-ajoneuvotyypit.px',
    'px_topic': 'Ympäristötilastot/05_Liikenne/2_Liikennemaarat/L7_suorite_ajoneuvotyypit.px',
    'indicator_id': 29,
    'query': 'Ajoneuvotyyppi == "Henkilöautot (milj. km)" & Katuluokka == "Yhteensä"',
}, {
    'name': 'Työkoneiden kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 261,
    'skip': True,
    # FIXME: Palataan vuodenvaihteen jälkeen
    # Otetaan ehkä SYKE:n datasta?
}, {
    'name': 'Sähkönkulutuksen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 30,
    'query': 'Alue == "Helsinki" & Sektori == "Kulutussähkö" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
}, {
    'name': 'Öljylämmityksen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/i01-kulutusperusteiset-kokonais-asukaskohtaiset.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/I01_kulutusperusteiset_kokonais_asukaskohtaiset.px',
    'indicator_id': 35,
    'query': 'Alue == "Helsinki" & Sektori == "Öljylämmitys" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
}, {
    'name': 'Liikenteen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 24,
    'query': 'Alue == "Helsinki" & Ajoneuvoluokka == "Yhteensä" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
}, {
    'name': 'Kaupungin omistamien kiinteistöjen sähkönkulutus',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/4-kaupungin-omistamat/e21-kaup-rakennus-tyypeittain.px',
    'px_topic': 'Ympäristötilastot/02_Energia/4_kaupungin_omistamat/E21_kaup_rakennus_tyypeittain.px',
    'indicator_id': 94,
    'query': 'Rakennustyyppi == "Kaikki yhteensä" & Muuttuja == "Sähkö (GWh)"'
}, {
    'name': 'Jakeluliikenteen kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 211,
    # FIXME: Pelkkä pakettiautot?
    'query': 'Alue == "Helsinki" & Ajoneuvoluokka == "Pakettiautot" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
}, {
    'name': 'Henkilöautojen pienhiukkaspäästöt Helsingissä',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/03-ilmanlaatu/1-ympariston-kuormitus/l012-paastot-ajoneuvot.px',
    'px_topic': 'Ympäristötilastot/03_Ilmanlaatu/1_Ympariston_kuormitus/L012_paastot_ajoneuvot.px',
    'indicator_id': 15,
    'query': 'Kunta == "Helsinki" & Katutyyppi == "Tiet ja kadut yht." & Ajoneuvoryhmä == "Henkilöautot yht." & Päästö == "Hiukkaset (t)"',
    'unit': 't/year',
}, {
    'name': 'Rakennusala rakennustyypeittäin',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/01-maankaytto/2-1-rakennukset/a01s-hki-rakennuskanta.px',
    'px_topic': 'Ympäristötilastot/01_Maankaytto/2_1_rakennukset/A01S_HKI_Rakennuskanta.px',
    'indicator_id': 12,
    'skip': True,
    # FIXME: Remove indicator
}, {
    'name': 'Rakennusala lämmitysmuodoittain',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/2-rakennuskanta/e25-lammitysmuodot-uudisrakennustyyppi.px',
    'px_topic': 'Ympäristötilastot/02_Energia/2_Rakennuskanta/E25_lammitysmuodot_uudisrakennustyyppi.px',
    'indicator_id': 13,
    'skip': True,
    # FIXME: Remove indicator
}, {
    'name': 'Helsingin rakennusala',
    'px_file': 'data/aluesarjat_px/helsingin-seudun-tilastot/helsingin-seutu/asunto-ja-rakennuskanta/rakennuskanta/hginseutu-ar-rr01-rakennukset-kayttark-rvuosi.px',
    'px_topic': 'Helsingin seudun tilastot/Helsingin seutu/Asunto ja rakennuskanta/Rakennuskanta/Hginseutu_AR_RR01_Rakennukset_kayttark_rvuosi.px',
    'indicator_id': 68,
    'query': 'Alue == "Helsinki" & `Käyttötarkoitus ja kerrosluku` == "Kaikki rakennukset" & Yksikkö == "Kerrosala" & Valmistumisvuosi == "Yhteensä"',
    'unit': 'M m2',
    # FIXME: --> Helsingin rakennusten kerrosala
    'map': lambda x: x / 1000000,
}, {
    'name': 'Raideliikenteen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 67,
    'query': 'Alue == "Helsinki" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'lambda_over': 'Ajoneuvoluokka',
    'lambda': lambda x: x["Lähijunat"] + x["Metrot"] + x["Raitiovaunut"],
}, {
    'name': 'Tieliikenteen kasvihuonekaasupäästöt (HSY)',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 23,
    'query': 'Alue == "Helsinki" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'lambda_over': 'Ajoneuvoluokka',
    'lambda': lambda x: x["Henkilöautot"] + x["Moottoripyörät"] + x["Pakettiautot"] + x["Kuorma-autot"] + x["Linja-autot"],
}, {
    'name': 'Joukkoliikenteen kasvihuonekaasupäästöt',
    'px_file': 'data/aluesarjat_px/ymparistotilastot/02-energia/3-energiaperainen-kuormitus/l1-liikenne-khk-paastot.px',
    'px_topic': 'Ympäristötilastot/02_Energia/3_Energiaperainen_kuormitus/L1_Liikenne_KHK_paastot.px',
    'indicator_id': 151,
    'query': 'Alue == "Helsinki" & Muuttuja == "Kokonaispäästöt (1000t CO2-ekv.)"',
    'lambda_over': 'Ajoneuvoluokka',
    # FIXME: --> Laivat otettiin pois
    'lambda': lambda x: x["Linja-autot"] + x["Lähijunat"] + x["Metrot"] + x["Raitiovaunut"],
}]


result = client.execute(GET_PLAN_INDICATORS, variable_values=dict(plan='hnh2035'))

indicators = result['planIndicators']
indicators_by_name = {x['name']: x for x in result['planIndicators']}
indicators_by_id = {int(x['id']): x for x in result['planIndicators']}


topic_latest_years = {}


def post_indicator_values(ind, s):
    pprint(ind)
    s.index = s.index.map(lambda x: str(x) + '-12-31')
    post_values(ind['id'], s)


def update_indicator(ind):
    parser = PxParser()
    print(ind['name'])
    if 'px_file' not in ind:
        print('\tFIXME')
        exit()

    with open(ind['px_file'], 'rb') as f:
        pxf = parser.parse(f.read())
    df = pxf.to_df(melt=True)

    iobj = indicators_by_id[ind['indicator_id']]
    # pprint(iobj)
    # print(df.tail())
    YEAR_COLS = ['vuosi', 'Vuosi']
    for col_name in df.columns:
        if col_name == 'value' or col_name in YEAR_COLS:
            continue
        vals = ["'%s'" % val for val in df[col_name].unique()]
        print('%s: %s' % (col_name, ', '.join(vals)))

    for col in YEAR_COLS:
        if col in df.columns:
            df = df.set_index(col)
            break
    else:
        raise Exception('Year col not found')

    if 'query' not in ind:
        pprint(iobj)
        print(df)
        input()
        print('\n')
        return

    df = df.query(ind['query'])
    if 'sum_over' in ind:
        df = df.drop(columns=ind['sum_over'])
        df = df.reset_index()
        cols = df.columns[:-1].values
        df = df.groupby(col, as_index=False)['value'].sum()
        df = df.set_index(cols[0])
    elif 'lambda_over' in ind:
        df = df.reset_index()
        df = df.set_index([list(df.columns)[0], ind['lambda_over']])
        for col in df.columns:
            if col == 'value':
                continue
            if len(df[col].unique()) != 1:
                raise Exception()
        df = df['value'].unstack(ind['lambda_over'])
        df = pd.DataFrame(dict(value=[ind['lambda'](x) for x in df.to_dict('records')]), index=df.index)

    s = df['value']
    assert s.index.is_unique
    s.index = s.index.astype(int)

    if ind.get('map'):
        s = s.map(ind['map'])

    s = s.loc[s != '..']

    latest_year = topic_latest_years.get(ind['px_topic'])
    series_newest = s.index.astype(int).max()
    if not latest_year or series_newest > latest_year:
        topic_latest_years[ind['px_topic']] = series_newest

    lv = iobj['latestValue']
    api_latest_year = int(lv['date'].split('-')[0]) if lv else None
    if not api_latest_year or series_newest > api_latest_year:
        post_indicator_values(iobj, s)

    print('\n')


def generate_indicator_table():
    df = pd.read_excel('data/Linkitykset_tilasto_ilmastovahti.xlsx')
    df = df[['Toimenpide', 'linkki rajapintaan']].dropna()
    topics = json.load(open('data/aluesarjat_px/ymparistotilastot.json', 'r', encoding='utf8'))
    for row in df.to_dict('records'):
        ind_name = row['Toimenpide']
        link = row['linkki rajapintaan'].strip('/')
        if 'api.' in link:
            fname = link.split('/')[-1]
            # print('%s:%s' % (ind_name, fname))
            for px_topic in topics:
                if fname in px_topic['topic_path']:
                    break
            else:
                print('No match in topics')
                exit()
            comment = None
        else:
            px_topic = None
            comment = link

        print("}, {")
        if comment:
            print("    # %s" % comment)

        ind = indicators_by_name.get(ind_name)
        if not ind:
            ind = indicators_by_name.get(ind_name + ' (HSY)')
            if ind:
                ind_name = ind_name + ' (HSY)'

        print("    'name': '%s'," % ind_name)
        if px_topic:
            print("    'px_file': '%s'," % px_topic['file'])
            print("    'px_topic': '%s'," % px_topic['topic_path'])
        if ind:
            print("    'indicator_id': %s," % ind['id'])

        # update_indicator(ind, px_topic)


if __name__ == '__main__':
    # generate_indicator_table()
    # exit()
    ok = 0

    matched = True
    # START_FROM = 'Helsingin rakennusala'
    START_FROM = None

    for ind in INDICATORS:
        if not matched:
            if ind['name'] == START_FROM:
                matched = True
            else:
                continue
        if ind.get('skip'):
            continue
        update_indicator(ind)
        ok += 1

    for key in sorted(topic_latest_years.keys()):
        print('%d: %s' % (topic_latest_years[key], key))

    print('OK: %d, total: %d' % (ok, len(INDICATORS)))
