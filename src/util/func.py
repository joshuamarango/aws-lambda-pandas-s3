import json
import os
from posix import environ
import dataframe_sql as sql
import awswrangler as wr
from ruamel import yaml

def sample_event() -> dict:
    """Get sample SQS event from local filesystem"""
    f_sample_event = open('./data/sample_event.json')
    json_sample_event = json.load(f_sample_event)
    f_sample_event.close()
    return json_sample_event

def get_table_schema(table_name:str) -> list:
    """Get the columns to filter by for a pandas Dataframe"""
    table_schemas = yaml.safe_load(open('./models/source/schema.yml'))
    return [col["name"] for col in table_schemas[table_name]["columns"]]

def get_sql_query(table_name:str) -> str:
    """Fetch SQL query file for generation of dim or fact table(s)"""
    f = open(f'./models/sql/{table_name}.sql')
    f_sql_query = f.read()
    f.close()
    return f_sql_query

def write_to_s3(tables:list) -> None:
    """Write Pandas DataFrame to S3 Datalake"""
    DATALAKE = os.environ.get("DATALAKE")
    DATABASE = os.environ.get("DATALAKE_DB")
    for table_name in tables:
        input_df = sql.query(get_sql_query(table_name))
        table_partitions = yaml.safe_load(open('./models/source/datalake.yml'))
        partition_cols = [p["name"] for p in table_partitions[table_name]["partitions"]]
        wr.s3.to_parquet(
            df=input_df,
            path=f"s3://{DATALAKE}/{DATABASE}/{table_name}",
            index=False,
            compression="gzip",
            use_threads=True,
            dataset=True,
            partition_cols=partition_cols,
            concurrent_partitioning=True,
            mode="append"
        )