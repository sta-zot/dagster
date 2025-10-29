import yaml
import pandas as pd
from etl.tools import revert_dict, Mapping
from etl.models import Meta
from etl.config import PACKAGE_ROOT as root
from etl.models import Minio

with open(root/"configs/mapping.yaml", encoding="utf-8") as yf:
    SCHEMA = yaml.safe_load(yf)


map = Mapping(SCHEMA)
minio = Minio()
# file = minio.get("1/event_report_1.xlsx")
event_df = pd.read_excel(minio.get("1/event_report_1.xlsx"))
event_df = event_df.rename(columns=map.get)
print(list(event_df.columns))
# cdp_df = pd.read_excel(minio.get("4/cdp_report_1.xlsx"))
# cdp_df = cdp_df.rename(columns=map.get)
# print(cdp_df.columns)
# edu_df = pd.read_excel(minio.get("2/edu_report_1.xlsx"))
# edu_df = edu_df.rename(columns=map.get)
# print(edu_df.columns)
# imp_df = pd.read_excel(minio.get("3/imp_report_1.xlsx"))
# imp_df = imp_df.rename(columns=map.get)
# print(imp_df.columns)
#print(SCHEMA)
#print(revert_dict(SCHEMA))


