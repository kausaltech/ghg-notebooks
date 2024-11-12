from __future__ import annotations

import os
from pathlib import Path

from dotenv import load_dotenv

_ = load_dotenv()

MUNICIPALITY_ID = os.getenv("MUNICIPALITY_ID") or "091"  # default to Helsinki
DATA_DIR = os.getenv("DATA_DIR") or Path(__file__).parent.resolve()

FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")
POSTGRESQL_DSN = os.getenv("POSTGRESQL_DSN")
NUUKA_API_BASE_URL = os.getenv("NUUKA_API_BASE_URL")
NUUKA_API_KEY = os.getenv("NUUKA_API_KEY")

MML_API_KEY = os.getenv('MML_API_KEY')
FMI_API_KEY = os.getenv('FMI_API_KEY')

DVC_PANDAS_REPOSITORY = os.getenv('DVC_PANDAS_REPOSITORY', 'git@github.com:kausaltech/dvctest.git')
DVC_PANDAS_DVC_REMOTE = os.getenv('DVC_PANDAS_DVC_REMOTE', 'kausal-s3')

def _path_from_env(name: str) -> Path | None:
    path = os.getenv(name)
    if path is None:
        return None
    return Path(path)

GITHUB_SSH_PRIVATE_KEY = _path_from_env('GITHUB_SSH_PRIVATE_KEY')
GITHUB_SSH_PUBLIC_KEY = _path_from_env('GITHUB_SSH_PUBLIC_KEY')

GITHUB_DATASET_TOKEN = os.getenv('GITHUB_DATASET_TOKEN')
GITHUB_USERNAME = os.getenv('GITHUB_USERNAME')

TIME_SERIES_DB_URL = os.getenv('TIME_SERIES_DB_URL')
