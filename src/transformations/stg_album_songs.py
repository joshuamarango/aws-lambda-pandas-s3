import pandas as pd
from util import func

def run(df:pd.DataFrame) -> pd.DataFrame:
    return (
        pd.json_normalize(
            df["details"]
                .explode("details")
        )
        [func.get_table_schema("stg_album_songs")]
    )