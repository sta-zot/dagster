import yaml
from etl.tools import revert_dict, Mapping
from etl.models import Meta
from etl.config import PACKAGE_ROOT as root

with open(root/"configs/mapping.yaml", encoding="utf-8") as yf:
    SCHEMA = yaml.safe_load(yf)


map = Mapping(SCHEMA)
#print(SCHEMA)
#print(revert_dict(SCHEMA))

meta = Meta(
    document_id="XXX",
    status="test",
    reason="Some reason"
)
print(meta)
print(map.get("облсть"))
print(map.get("облость"))
print(map.get("област"))
