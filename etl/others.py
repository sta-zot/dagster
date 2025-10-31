import yaml
import pandas as pd
import re
from etl.tools import revert_dict, Mapping, load_location_to_db, load_date_dim_to_db
from etl.models import Meta
from etl.config import PACKAGE_ROOT as root
from etl.models import Minio, DWHModel
from sqlalchemy import create_engine, text
from etl.pipelines import fetch_all_new_files

with open(root / "configs/mapping.yaml", encoding="utf-8") as yf:
    MAPPING_SCHEMA = yaml.safe_load(yf)

with open(root / "configs/schema.yaml", "r", encoding="utf-8") as f:
    dwh_schema = yaml.safe_load(f)


# engine = create_engine("postgresql://admin:admin@10.0.5.89:5433/testdb")
# load_location_to_db(engine)
# load_date_dim_to_db(engine)
mapping_schema = {key: MAPPING_SCHEMA[key]["matches"] for key in MAPPING_SCHEMA.keys()}


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
volunteers = []
excluded_cols = ['volunteers', 'volunteers_type']
volunteers_key_lookup_cols = [
    col for col in event_report.columns if col not in excluded_cols
    ]    
for _, row in event_report.iterrows():
    _volunteers = row['volunteers'].split(', ')
    
    for volunteer in _volunteers:
        record= {
                "name": re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]','', volunteer).strip(),
                "type":  re.sub(r'[^а-яА-ЯёЁa-zA-Z\s]','', row['volunteers_type']).strip(),
            }
        for col in volunteers_key_lookup_cols:
                record[col] = row[col]
        volunteers.append(record)
df_volunteers = pd.DataFrame(volunteers)
# print(df_volunteers)

dwh = DWHModel(
    db_host="10.0.5.89",
    db_port=5433,
    db_name="testdb",
    db_user="admin",
    db_password="admin",
)
# neww = dwh.process_dims(
#     df=event_report,
#     table='dim_audience',
#     lookup_fields=['age_group','social_group'],
#     target_fields=['age_group','social_group'],
#     key_col='audience_id'
# )
# print(neww)
# test_fact_events = pd.DataFrame.from_dict()
test_dict = {
        'location_id': [0,1],
        'event': ['test', 'test2'],
        'event_id': [0,5],
        'audience_id':[0,34],
        "organizer_id": [0, 34],
        "partner_id": [0, 54],
        "date": [20250404, 20250405],
        'participants_cnt': [2,23],
    }
test_pd = pd.DataFrame.from_dict(test_dict)
print(test_pd)
test = dwh.load_facts(test_pd, "fact_events")
print(test)