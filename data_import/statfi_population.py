"""
Update the Statistics Finland population dataset (StatFin table 11re).

Downloads "Väestö iän (1-v.) ja sukupuolen mukaan alueittain" (population by one-year age group, sex and
municipality) and pushes it to the DVC repository as `statfi/StatFin/vrm/vaerak/statfin_vaerak_pxt_11re`.
The dataset is used by the Paths node `finland.Population`, which sums the population per year for one
municipality, so the dataset contains only detail rows: no "KOKO MAA" row and no "Yhteensä" rows for age or sex.

Run from the repository root:

    python data_import/statfi_population.py [--dry-run]
"""
from __future__ import annotations

import argparse
import sys
import time
from datetime import datetime
from pathlib import Path
from zoneinfo import ZoneInfo

import polars as pl
import requests
from dvc_pandas.dataset import Dataset, DatasetMeta

# Allow running this file directly as `python data_import/statfi_population.py`
sys.path.insert(0, str(Path(__file__).resolve().parent.parent))

from utils.dvc import get_repo  # noqa: E402

DATASET_ID = "statfi/StatFin/vrm/vaerak/statfin_vaerak_pxt_11re"
API_BASE_URL = "https://pxdata.stat.fi/PxWeb/api/v1/fi/StatFin/vaerak"
# The table used to be called statfin_vaerak_pxt_11re.px (hence the dataset identifier), but that id now returns
# HTTP 400. The table is listed at API_BASE_URL as 11re.px.
TABLE_ID = "11re.px"
TABLE_URL = f"{API_BASE_URL}/{TABLE_ID}"

VALUE_COLUMN = "Väestö 31.12."
# PxWeb variable texts; the variable codes contain the classification date and change over time
AREA = "Alue"
AGE = "Ikä"
SEX = "Sukupuoli"
YEAR = "Vuosi"
CONTENTS = "Tiedot"
TOTAL_CODE = "SSS"  # "KOKO MAA" for areas, "Yhteensä" for age and sex
INDEX_COLUMNS = [YEAR, AREA, SEX, AGE]

# Timestamps in the PxWeb table listing are in Finnish local time
STATFI_TIMEZONE = ZoneInfo("Europe/Helsinki")
MAX_RETRIES = 5


def request(method: str, url: str, **kwargs) -> requests.Response:
    """Make a request to the PxWeb API, retrying when the API's rate limit is hit."""
    for attempt in range(MAX_RETRIES):
        response = requests.request(method, url, timeout=120, **kwargs)
        if response.status_code in (429, 503) and attempt < MAX_RETRIES - 1:
            time.sleep(10 * (attempt + 1))
            continue
        response.raise_for_status()
        return response
    raise AssertionError("unreachable")


def get_table_updated() -> datetime:
    """Return the time when Statistics Finland last updated the table, according to the table listing."""
    tables = request("GET", f"{API_BASE_URL}/").json()
    table = next((t for t in tables if t["id"] == TABLE_ID), None)
    if table is None:
        raise ValueError(f"Table {TABLE_ID} not found in the table listing at {API_BASE_URL}/")
    return datetime.fromisoformat(table["updated"]).replace(tzinfo=STATFI_TIMEZONE)


def get_variables() -> dict[str, dict]:
    """Return the table's variables keyed by their text (e.g. "Alue")."""
    metadata = request("GET", TABLE_URL).json()
    return {variable["text"]: variable for variable in metadata["variables"]}


def query_year(variables: dict[str, dict], year: str) -> pl.DataFrame:
    """
    Download the population of all municipalities by age and sex for one year.

    One year is about 62 000 cells, which fits within the PxWeb per-query cell limit.
    """
    selected_values = {
        AREA: [v for v in variables[AREA]["values"] if v != TOTAL_CODE],
        AGE: [v for v in variables[AGE]["values"] if v != TOTAL_CODE],
        SEX: [v for v in variables[SEX]["values"] if v != TOTAL_CODE],
        YEAR: [year],
        CONTENTS: variables[CONTENTS]["values"],
    }
    query = {
        "query": [
            {"code": variables[text]["code"], "selection": {"filter": "item", "values": values}}
            for text, values in selected_values.items()
        ],
        "response": {"format": "json-stat2"},
    }
    data = request("POST", TABLE_URL, json=query).json()

    # json-stat2 values are in row-major order over the dimensions listed in `id`
    labels: list[list[str]] = []
    for dimension_id in data["id"]:
        category = data["dimension"][dimension_id]["category"]
        codes = sorted(category["index"], key=category["index"].get)
        labels.append([category["label"][code] for code in codes])
    code_to_text = {variables[text]["code"]: text for text in selected_values}
    index_df = pl.DataFrame({code_to_text[data["id"][0]]: labels[0]})
    for dimension_id, dimension_labels in zip(data["id"][1:], labels[1:]):
        index_df = index_df.join(pl.DataFrame({code_to_text[dimension_id]: dimension_labels}), how="cross")

    values = data["value"]
    if len(values) != len(index_df) or any(v is None for v in values):
        raise ValueError(f"Missing values in the response for year {year}")
    return index_df.with_columns(pl.Series(VALUE_COLUMN, values, dtype=pl.Int64))


def get_population_df() -> pl.DataFrame:
    variables = get_variables()
    contents = variables[CONTENTS]["valueTexts"]
    if contents != [VALUE_COLUMN]:
        raise ValueError(f"Unexpected table contents: {contents}")

    dfs = []
    for year in variables[YEAR]["values"]:
        print(f"Downloading {year}")
        dfs.append(query_year(variables, year))
    df = pl.concat(dfs)

    # Same schema as the earlier versions of the dataset
    return df.select(
        pl.col(VALUE_COLUMN).cast(pl.Int64),
        pl.col(YEAR).cast(pl.Utf8),
        pl.col(AREA).cast(pl.Categorical),
        pl.col(SEX).cast(pl.Categorical),
        pl.col(AGE).cast(pl.Categorical),
    )


def push_dataset_to_dvc(df: pl.DataFrame, modified_at: datetime) -> None:
    meta = DatasetMeta(
        identifier=DATASET_ID,
        index_columns=INDEX_COLUMNS,
        modified_at=modified_at,
    )
    repo = get_repo()
    repo.push_dataset(Dataset(df, meta=meta))


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    parser.add_argument("--dry-run", action="store_true", help="download and format the data, but don't push it")
    args = parser.parse_args()

    modified_at = get_table_updated()
    print(f"Table {TABLE_ID} was last updated at {modified_at.isoformat()}")

    df = get_population_df()
    print(df)
    print(df.group_by(YEAR).agg(pl.col(VALUE_COLUMN).sum()).sort(YEAR).tail(5))

    if args.dry_run:
        print("Dry run; not pushing the dataset")
    else:
        print(f"Pushing dataset {DATASET_ID} to DVC")
        push_dataset_to_dvc(df, modified_at)
        print("Done!")
