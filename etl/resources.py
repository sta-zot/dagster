from dagster import resource
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

# @resource
# def target_db_resource(context):
#     from sqlalchemy import create_engine
#     return create_engine(EnvVar("TARGET_DB_URL").get_value())