import yaml
import pandas as pd
from etl.tools import revert_dict, Mapping
from etl.models import Meta
from etl.config import PACKAGE_ROOT as root
from etl.models import Minio, DWHModel
from sqlalchemy import create_engine, text
from etl.pipelines import fetch_all_new_files

with open(root/"configs/mapping.yaml", encoding="utf-8") as yf:
    MAPPING_SCHEMA = yaml.safe_load(yf)

with open(root/"configs/schema.yaml", 'r', encoding='utf-8') as f:
    dwh_schema = yaml.safe_load(f)

# dim_location_df = pd.read_csv(root/"data/locations.csv")
# print(dim_location_df.columns)
# dim_location_df.drop(['id'], inplace=True, axis=1)
# print(dim_location_df.columns)
# engine = create_engine("postgresql://admin:admin@10.0.5.89:5433/testdb")

# dim_location_df.to_sql('dim_location', engine, if_exists='append', index=False)

mapping_schema = ({key: MAPPING_SCHEMA[key]['matches'] for key, _ in MAPPING_SCHEMA.items()})
dwh = DWHModel(
    db_host="10.0.5.89",
    db_port=5433,
    db_name="testdb",
    db_user="admin",
    db_password="admin",
)
minio = Minio()
mapping = Mapping(mapping_schema)
file_event_report_1 = "1/event_report_1.xlsx"
event_report = pd.read_excel(minio.get(file_event_report_1))
# print(event_report.columns)
event_report = event_report.rename(columns=mapping.get)
event_report['settlement'] = (
    event_report['settlement']
    .str.strip()
    .str.replace(r'^[\w]+\.', '', regex=True)
    .str.strip())
# print(event_report.columns) 

ids = dwh.process_dims(
    df=event_report,
    table="dim_location",
    lookup_fields=["region", "settlement"],
    target_fields=["region", "municipality",'settlement'],
    key_col="location_id"
)
print(f'RESULT: \n {ids.columns}')
ids = dwh.process_dims(
    df=ids,
    table="dim_audience",
    lookup_fields=["social_group", "age_group"],
    target_fields=["social_group", "age_group"],
    key_col="audience_id"
)
print(f'RESULT: \n {ids.columns}')
ids = dwh.process_dims(
    df=ids,
    table="dim_partner",
    lookup_fields=["partner_name", "partner_type"],
    target_fields=["partner_name", "partner_type"],
    key_col="partner_id"
)
print(f'RESULT: \n {ids.columns}')
ids = dwh.process_dims(
    df=ids,
    table="dim_event",
    lookup_fields=["event_type", "event_format", "event_topic", ],
    target_fields=["event_type", "event_format", "event_topic",],
    key_col="dim_event"
)
print(f'RESULT: \n {ids.columns}')