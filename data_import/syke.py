"""Update SYKE data."""
from __future__ import annotations

import shutil
import tempfile
from datetime import datetime, timezone
from pathlib import Path
from typing import cast

import polars as pl
import requests
from dvc_pandas.dataset import Dataset, DatasetMeta
from pint import UnitRegistry, get_application_registry

from utils.dvc import get_repo

# When paastot.hiilineutraalisuomi.fi has updated its data the last time
# Can be found out from "Last modified" timestamps in
# https://paastot.hiilineutraalisuomi.fi/Tulokset_taso_5/
MODIFIED_AT = datetime(2026, 6, 8, tzinfo=timezone.utc)
DATASET_ID = "syke/alas_emissions"

# Original data has abbreviations for the values in columns "taso_3", "taso_4"
# and "taso_5"
TASO_3_FULL_NAMES = {
    "Kul": "Kulutussähkö",
    "Säh": "Sähkölämmitys",
    "Kau": "Kaukolämpö",
    "Ölj": "Öljylämmitys",
    "Muu": "Muu lämmitys",
    "Teo": "Teollisuus",
    "Työ": "Työkoneet",
    "Tie": "Tieliikenne",
    "Rai": "Raideliikenne",
    "Ves": "Vesiliikenne",
    "Maa": "Maatalous",
    "Jät": "Jätteiden käsittely",
    "F-k": "F-kaasut",
    "Tuu": "Päästöhyvitykset"
}

TASO_4_FULL_NAMES = {
    "Kul": "Kulutussähkö",
    "Säh": "Sähkölämmitys",
    "Kau": "Kaukolämpö",
    "Ölj": "Öljylämmitys",
    "Muu": "Muu lämmitys",
    "MaL": "Maalämpö",
    "Puu": "Puulämmitys",
    "Kad": "Kadut",
    "Ti": "Tiet",
    "TaL": "Tavaraliikenne",
    "HeL": "Henkilöliikenne",
    "LäL": "Lähiliikenne",
    "Läp": "Läpiajoliikenne",
    "MoM": "Moottoripyörät ja mopot",
    "HuV": "Huviveneet",
    "Lai": "Laivat",
    "TyV": "Työveneet",
    "Teo": "Teollisuus",
    "Työ": "Työkoneet",
    "Maa": "Maatalous",
    "Kot": "Kotieläimet",
    "Pel": "Peltoviljely",
    "Kom": "Kompostointi",
    "Mäd": "Mädätys",
    "JäP": "Jätevesien puhdistus",
    "Kaa": "Kaatopaikat",
    "AjI": "Ajoneuvojen ilmastointilaitteet",
    "RaI": "Rakennusten ilmastointilaitteet",
    "KAK": "Kaupan ja ammattikeittiöiden kylmälaitteet",
    "MuF": "Muut F-kaasujen lähteet",
    "Tuu": "Tuulivoima"
}

TASO_5_FULL_NAMES = {
    "Asu": "Asuminen",
    "Pal": "Palvelut",
    "Teo": "Teollisuus",
    "Maa": "Maatalous",
    "TeP": "Teollisuuden polttoaineet",
    "MuT": "Muut työkoneet",
    "MMK": "Maa- ja metsätalouskoneet",
    "RaK": "Rakennustyökoneet",
    "Kai": "Kaivos- ja teollisuustyökoneet",
    "TiK": "Tietyökoneet",
    "HuV": "Huviveneet",
    "MaL": "Matkustajaliikenne",
    "TaL": "Tavaraliikenne",
    "Kal": "Kalastusalukset",
    "Lau": "Lautat ja lossit",
    "TyV": "Työveneet ja alukset",
    "Elä": "Eläinten ruuansulatus",
    "Lai": "Laidunnus",
    "Epä": "Epäorgaaniset lannoitteet",
    "Lan": "Lannankäsittely",
    "Ma": "Maaperä",
    "MuV": "Muut viljelysmaiden päästöt",
    "Org": "Orgaaniset lannoitteet",
    "Yhd": "Yhdyskuntajäte",
    "KAK": "Kaupan ja ammattikeittiöiden kylmälaitteet",
    "RaI": "Rakennusten ilmastointilaitteet",
    "AjI": "Ajoneuvojen ilmastointilaitteet",
    "MuF": "Muut F-kaasujen lähteet",
    "Hen": "Henkilöautot",
    "KuA": "Kuorma-autot",
    "LiA": "Linja-autot",
    "PaA": "Pakettiautot",
    "Läp": "Läpiajoliikenne",
    "MoM": "Moottoripyörät ja mopot",
    "Met": "Metrot ja raitiovaunut",
    "LäJ": "Lähijunat",
    "Die": "Diesel",
    "Sä": "Sähkö",
    "Tuu": "Tuulivoima"
}

# taso_1 and taso_2 values are missing from the original data, but we can deduce
# them from taso_3 values
TASO_3_TO_TASO_1_AND_2_MAPPINGS = {
    "Kulutussähkö": ["Energia", "Kulutussähkö"],
    "Sähkölämmitys": ["Energia", "Rakennusten lämmitys"],
    "Kaukolämpö": ["Energia", "Rakennusten lämmitys"],
    "Öljylämmitys": ["Energia", "Rakennusten lämmitys"],
    "Muu lämmitys": ["Energia", "Rakennusten lämmitys"],
    "Teollisuus": ["Energia", "Teollisuus ja työkoneet"],
    "Työkoneet": ["Energia", "Työkoneet"],
    "Tieliikenne": ["Energia", "Liikenne"],
    "Raideliikenne": ["Energia", "Liikenne"],
    "Vesiliikenne": ["Energia", "Liikenne"],
    "Maatalous": ["Maatalous", "Maatalous"],
    "Jätteiden käsittely": ["Jätteiden käsittely", "Jätteiden käsittely"],
    "F-kaasut": ["Teollisuusprosessit", "F-kaasut"],
    "Päästöhyvitykset": ["Kompensaatiot", "Kompensaatiot"],
}

_municipalities_info_df_cache: pl.DataFrame | None = None


def get_municipalities_info_path(data_dir: Path) -> Path:
    """Get the path to the municipalities info file."""
    return data_dir / "kuntainfo_ja_asukasluvut.csv"


def get_municipality_data_path(data_dir: Path, municipality_number: int) -> Path:
    """Get the path to the municipality data file."""
    return data_dir / f"kunta_{municipality_number}.csv"


def get_municipalities_info_df(data_dir: Path) -> pl.DataFrame:
    """Get a dataframe with municipality number, municipality name and province name."""
    global _municipalities_info_df_cache

    if _municipalities_info_df_cache is None:
        df = pl.read_csv(
            get_municipalities_info_path(data_dir),
            has_header=True,
            separator=';',
            decimal_comma=True,
            null_values=['NA', 'NULL', ''],
        )

        df = df.select(
            pl.col("kuntanro").alias("kuntanumero"),
            pl.col("kunta_fi").alias("kunta"),
            pl.col("maakunta_fi").alias("maakunta")
        )

        _municipalities_info_df_cache = df

    return _municipalities_info_df_cache


def get_municipality_numbers(data_dir: Path) -> list[int]:
    """Get the municipality numbers from the municipalities info dataframe."""
    df = get_municipalities_info_df(data_dir)
    municipality_numbers = df['kuntanumero'].to_list()
    return municipality_numbers


def download_data(data_dir: Path | None = None) -> Path:
    """
    Download the data from paastot.hiilineutraalisuomi.fi.

    If data_dir is not given, the data is downloaded to a temporary directory.
    The user is responsible for deleting the directory.

    Returns the path to the data directory.
    """
    if data_dir is None:
        data_dir = Path(tempfile.mkdtemp())

    # Download the municipalities info
    municipalities_info_url = "https://paastot.hiilineutraalisuomi.fi/asukasluvut_2025.csv"
    municipalities_info_path = get_municipalities_info_path(data_dir)
    response = requests.get(municipalities_info_url)
    municipalities_info_path.write_text(response.content.decode('utf-8'))

    # Download the data for all municipalities
    for municipality_number in get_municipality_numbers(data_dir):
        municipality_data_url = f"https://paastot.hiilineutraalisuomi.fi/Tulokset_taso_5/kunta_{municipality_number}.csv"
        municipality_data_path = get_municipality_data_path(data_dir, municipality_number)
        response = requests.get(municipality_data_url)
        municipality_data_path.write_text(response.content.decode('utf-8'))

    return data_dir


def prepare_municipality_data(data_dir: Path, municipality_number: int) -> pl.DataFrame:
    """Read municipality data and prepare the data to wanted format."""

    df = pl.read_csv(
        get_municipality_data_path(data_dir, municipality_number),
        has_header=True,
        separator=';',
        decimal_comma=True,
        null_values=['NA', 'NULL', ''],
    )

    # Add municipality number to the data
    df = df.with_columns(pl.lit(municipality_number).alias("kuntanumero"))

    # Fill in municipality and province names
    info_df = get_municipalities_info_df(data_dir)
    municipality_info_row = info_df.filter(pl.col("kuntanumero") == municipality_number)
    municipality_name = municipality_info_row['kunta'].item()
    province_name = municipality_info_row['maakunta'].item()
    df = df.with_columns(
        pl.lit(municipality_name).alias("kunta"),
        pl.lit(province_name).alias("maakunta"),
    )

    return df


def get_syke_data_df(data_dir: Path) -> pl.DataFrame:
    """Get the syke data dataframe."""

    # Read all municipality data files and stack them together
    df = pl.DataFrame()
    for municipality_number in get_municipality_numbers(data_dir):
        df = df.vstack(prepare_municipality_data(data_dir, municipality_number))

    # Rename columns
    df = df.rename({
        "kt": "ktCO2e",
        "kt_tuuli": "ktCO2e_tuuli",
        "GWh": "energiankulutus",
        "hinku_laskenta": "hinku-laskenta",
    })

    # Map the abbreviated values in columns "taso_3", "taso_4" and "taso_5" to full class names
    df = df.with_columns(
        pl.col("taso_3").map_elements(TASO_3_FULL_NAMES.get, return_dtype=pl.Utf8),
        pl.col("taso_4").map_elements(TASO_4_FULL_NAMES.get, return_dtype=pl.Utf8),
        pl.col("taso_5").map_elements(TASO_5_FULL_NAMES.get, return_dtype=pl.Utf8),
    )

    # Fill in taso_1 and taso_2 columns based on taso_3 values
    df = df.with_columns([
        pl.col("taso_3").map_elements(
            lambda key: TASO_3_TO_TASO_1_AND_2_MAPPINGS.get(key, [None, None])[0],
            return_dtype=pl.Utf8
        ).alias("taso_1"),
        pl.col("taso_3").map_elements(
            lambda key: TASO_3_TO_TASO_1_AND_2_MAPPINGS.get(key, [None, None])[1],
            return_dtype=pl.Utf8
        ).alias("taso_2")
    ])

    # Create "muni" column
    df = df.with_columns(pl.col('kunta').str.to_lowercase().alias('muni'))

    # Cast column types
    df = df.with_columns(
        pl.col("kuntanumero").cast(pl.Int32),
        pl.col("ktCO2e").cast(pl.Float64),
        pl.col("ktCO2e_tuuli").cast(pl.Float64),
        pl.col("energiankulutus").cast(pl.Float64),
        pl.col("muni").cast(pl.Utf8),
        pl.col("vuosi").cast(pl.Int32),
        pl.col("hinku-laskenta").cast(pl.Boolean),
        pl.col("päästökauppa").cast(pl.Boolean),
        pl.col("kunta").cast(pl.Categorical),
        pl.col("maakunta").cast(pl.Categorical),
        pl.col("taso_1").cast(pl.Categorical),
        pl.col("taso_2").cast(pl.Categorical),
        pl.col("taso_3").cast(pl.Categorical),
        pl.col("taso_4").cast(pl.Categorical),
        pl.col("taso_5").cast(pl.Categorical),
    )

    # Order the dataframe for nicer output
    df = df.select([
        "kuntanumero",
        "ktCO2e",
        "ktCO2e_tuuli",
        "energiankulutus",
        "muni",
        "vuosi",
        "hinku-laskenta",
        "päästökauppa",
        "kunta",
        "maakunta",
        "taso_1",
        "taso_2",
        "taso_3",
        "taso_4",
        "taso_5",
    ])
    return df


def push_dataset_to_dvc(df: pl.DataFrame) -> None:
    """Push the municipality data to DVC repo."""
    unit_registry: UnitRegistry = cast("UnitRegistry", get_application_registry().get())
    units: dict[str, str] = {
        'ktCO2e': str(unit_registry.parse_units('Gg/a')),
        'ktCO2e_tuuli': str(unit_registry.parse_units('Gg/a')),
        'energiankulutus': str(unit_registry.parse_units('GWh/a')),
    }

    index_columns = [
        "vuosi",
        "hinku-laskenta",
        "päästökauppa",
        "kunta",
        "maakunta",
        "taso_1",
        "taso_2",
        "taso_3",
        "taso_4",
        "taso_5",
    ]

    meta = DatasetMeta(
        identifier=DATASET_ID,
        units=units,
        index_columns=index_columns,
        modified_at=MODIFIED_AT,
    )

    repo = get_repo()
    repo.push_dataset(Dataset(df, meta=meta))


if __name__ == '__main__':

    print("Downloading data from paastot.hiilineutraalisuomi.fi")
    tmp_dir = download_data()
    print(f"Data downloaded to {tmp_dir}.")

    print("Formatting the data")
    df = get_syke_data_df(tmp_dir)
    print("Data formatted.")
    print(df)

    print("Pushing data to DVC")
    push_dataset_to_dvc(df)
    print("Dataset pushed.")

    print("Removing the downloaded files")
    shutil.rmtree(tmp_dir)

    print("Done!")
