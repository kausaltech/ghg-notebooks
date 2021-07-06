import os
from dotenv import load_dotenv

load_dotenv()


MUNICIPALITY_ID = os.getenv("MUNICIPALITY_ID") or "091"  # default to Helsinki
DATA_DIR = os.getenv("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))

QUILT_USER = os.getenv("QUILT_USER")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")
POSTGRESQL_DSN = os.getenv("POSTGRESQL_DSN")
NUUKA_API_BASE_URL = os.getenv("NUUKA_API_BASE_URL")
NUUKA_API_KEY = os.getenv("NUUKA_API_KEY")

MML_API_KEY = os.getenv('MML_API_KEY')
FMI_API_KEY = os.getenv('FMI_API_KEY')

DVC_PANDAS_REPOSITORY = os.getenv('DVC_PANDAS_REPOSITORY', 'git@github.com:kausaltech/dvctest.git')
DVC_PANDAS_DVC_REMOTE = os.getenv('DVC_PANDAS_DVC_REMOTE', 'kausal-s3')
