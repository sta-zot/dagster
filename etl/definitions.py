from dagster import Definitions, mem_io_manager  #, load_assets_from_modules
#from etl import assets  # noqa: TID252
from etl import resources  # noqa: TID252etl\resources.py
from etl.pipelines import graph  # noqa: TID252
#all_assets = load_assets_from_modules([assets])

defs = Definitions(
    #assets=all_assets,
    resources={
        "mongo_client": resources.mongo_client_resource,
        "s3_client": resources.s3_client_resource,
        "target_db": resources.target_db_resource,
        "io_manager": mem_io_manager,
    },
    jobs=[
        graph.process_all_new_files
    ]
)
