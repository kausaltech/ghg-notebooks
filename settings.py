import os
from dotenv import load_dotenv

load_dotenv()


QUILT_USER = os.getenv("QUILT_USER")
FINGRID_API_KEY = os.getenv("FINGRID_API_KEY")
INFLUXDB_DSN = os.getenv("INFLUXDB_DSN")
POSTGRESQL_DSN = os.getenv("POSTGRESQL_DSN")
