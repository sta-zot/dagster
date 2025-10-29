# etl/etl/config.py
from pathlib import Path
from dotenv import load_dotenv
from os import getenv



# Путь к директории etl/etl (где лежит этот файл)
PACKAGE_ROOT = Path(__file__).parent.resolve()
load_dotenv()
# Путь к configs
CONFIGS_DIR = PACKAGE_ROOT / "configs"


MONGO_HOST = getenv("DS_HOST", "localhost")
MONGO_PORT = getenv("DS_PORT", 27017)
MONGO_DB = getenv("DS_DB", "reports")
MONGO_USER = getenv("DS_USER", "admin")
MONGO_PASSWORD = getenv("DS_PASSWD", "admin123")

S3_ACCESS_KEY = getenv("S3_ACCESS_KEY", "minioadmin")
S3_SECRET_KEY = getenv("S3_SECRET_KEY", "minioadmin")
S3_BUCKET = getenv("S3_BUCKET", "reports")
S3_ENDPOINT_URL =  getenv("S3_ENDPOINT_URL",'http://minio:9000')
