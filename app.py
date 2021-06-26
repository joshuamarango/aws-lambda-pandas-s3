import pandas as pd
import dataframe_sql as sql
from src.util import func
from src.transformations import (
    stg_albums,
    stg_album_songs,
    stg_vendors
)

def lambda_handler(event, context):
    
    # Load raw data from AWS SNS into Pandas DataFrame
    df_raw_albums = pd.DataFrame.from_dict(event["Records"][0]["Sns"]["Message"])
    
    """Section: Example of initial transformations & SQL registration for staging tables"""
    # stg_albums
    sql.register_temp_table(
        frame=stg_albums.run(df_raw_albums),
        table_name="stg_albums"
    )
    # stg_album_songs
    sql.register_temp_table(
        frame=stg_album_songs.run(df_raw_albums),
        table_name="stg_album_songs"
    )
    # stg_vendors
    sql.register_temp_table(
        frame=stg_vendors.run(df_raw_albums),
        table_name="stg_vendors"
    )
    
    # Write dim tables to datalake
    func.write_to_s3(tables=[
        "dim_albums",
        "dim_album_songs",
        "dim_vendors"
    ])
