import pandas as pd
import dataframe_sql as sql
from src import util

def lambda_handler(event, context=None):
    
    # Load raw data from AWS SNS into Pandas DataFrame
    df_raw_albums = pd.DataFrame.from_dict(event["Records"][0]["Sns"]["Message"])
    
    """Section: Initial transformations & SQL Registration for staging tables"""
    # stg_albums
    sql.register_temp_table(
        frame=(
            df_raw_albums[util.get_table_schema("stg_albums")]
        ),
        table_name="stg_albums"
    )
    # stg_album_songs
    sql.register_temp_table(
        frame=(
            pd.json_normalize(
                df_raw_albums["details"]
                    .explode("details")
            )
            [util.get_table_schema("stg_album_songs")]
        ),
        table_name="stg_album_songs"
    )
    # stg_vendors
    sql.register_temp_table(
        frame=(
            df_raw_albums[["vendor"]]
                .join(pd.json_normalize(df_raw_albums.vendor))
                .drop_duplicates(subset=["id"])
                [util.get_table_schema("stg_vendors")]
        ),
        table_name="stg_vendors"
    )
    
    # Write dim tables to datalake
    util.write_to_s3(tables=[
        "dim_albums",
        "dim_album_songs",
        "dim_vendors"
    ])

lambda_handler(event=util.sample_event())
