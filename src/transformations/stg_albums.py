import pandas as pd
from util import func

def run(df:pd.DataFrame) -> pd.DataFrame:
    return (
        df[func.get_table_schema("stg_albums")]
    )