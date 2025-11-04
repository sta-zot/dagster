import yaml
import pandas as pd
# import re
from etl.tools import revert_dict, Mapping, load_location_to_db, load_date_dim_to_db
# from etl.models import Meta
from etl.config import PACKAGE_ROOT as root
from etl.models import Minio, DWHModel
# from sqlalchemy import create_engine, text
# from etl.pipelines import fetch_all_new_files
# from etl.pipelines import graph

with open(root / "configs/mapping.yaml", encoding="utf-8") as yf:
    MAPPING_SCHEMA = yaml.safe_load(yf)

# with open(root / "configs/schema.yaml", "r", encoding="utf-8") as f:
#     dwh_schema = yaml.safe_load(f)

# engine = create_engine("postgresql://admin:admin@10.0.5.89:5433/testdb")
# load_location_to_db(engine)
# load_date_dim_to_db(engine)
mapping_schema = {key: MAPPING_SCHEMA[key]["matches"] for key in MAPPING_SCHEMA.keys()}


def clean(df: pd.DataFrame) -> pd.DataFrame:
    df = df.rename(columns=mapping.get)
    df['settlement'] = (
        df['settlement']
        .str.strip()
        .str.replace(r'^[\w]+\.', '', regex=True)
        .str.strip())
    return df


minio = Minio()
mapping = Mapping(mapping_schema)
# file_event_report = "1/event_report_1.xlsx"
# file_imp_report = "3/imp_report_1.xlsx"
file_cdp_report = "4/cdp_report_1.xlsx"
# file_edu_report = "2/edu_report_1.xlsx"
# imp_report = pd.read_excel(minio.get(file_imp_report))
# event_report = pd.read_excel(minio.get(file_event_report))
cdp_report = pd.read_excel(minio.get(file_cdp_report))
# edu_report = pd.read_excel(minio.get(file_edu_report))
# # print(event_report.columns)
# imp_report = imp_report.rename(columns=mapping.get)
# imp_report['settlement'] = (
#     imp_report['settlement']
#     .str.strip()
#     .str.replace(r'^[\w]+\.', '', regex=True)
#     .str.strip())
# event_report = clean(event_report)
cdp_report = clean(cdp_report)
# edu_report = clean(edu_report)

dwh = DWHModel(
    db_host="10.0.5.89",
    db_port=5433,
    db_name="testdb",
    db_user="admin",
    db_pass="admin",
)
# # dwh.load_im_placements_facts(imp_report)  # -> Готово ✅ 
# # dwh.load_events_facts(event_report)  # -> Готово ✅
# # dwh.load_edu_integrations_facts(edu_report)
print(f"CDP columns before insert:\n \t {[cdp_report.columns]}" )
dwh.load_trainig_facts(cdp_report)
# # # print(f"CDP columns: \t {[cdp_report.columns]}" )
# # # print(f"EDU columns: \t {[edu_report.columns]}")


