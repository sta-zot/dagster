from os import getenv
from dagster import EnvVar, resource
from etl.models import Minio, MongoDB, DWHModel


# ==============================
# 1. РЕСУРСЫ (подключения к внешним системам)
# ==============================


@resource
def mongo_client_resource(context):
    return MongoDB()


@resource
def s3_client_resource(context):
    return Minio()


@resource
def target_db_resource(context):
    # db_url = (
    #     f"postgresql://{EnvVar('DB_USER')}:{EnvVar('DB_PASS')}
    #     f"@{EnvVar('DB_HOST')}:{EnvVar('DB_PORT')}/{EnvVar('DB_NAME')}"
    # )
    
    return DWHModel(
        db_host=getenv('DB_HOST'),
        db_port=getenv('DB_PORT'),
        db_name=getenv('DB_NAME'),
        db_user=getenv('DB_USER'),
        db_pass=getenv('DB_PASS'),
    )
    
    
if __name__ == "__main__":
    print(f"""
          {getenv('DB_HOST')}|
          {getenv('DB_PORT')}|
          {getenv('DB_NAME')}|
          {getenv('DB_USER')}|
          {getenv('DB_PASS')}
          """)