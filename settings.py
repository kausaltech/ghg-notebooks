import os
from dotenv import load_dotenv

load_dotenv()


MUNICIPALITY_ID = os.getenv("MUNICIPALITY_ID")
DATA_DIR = os.getenv("DATA_DIR") or os.path.dirname(os.path.abspath(__file__))

QUILT_USER = os.getenv("QUILT_USER")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")
POSTGRESQL_DSN = os.getenv("POSTGRESQL_DSN")
