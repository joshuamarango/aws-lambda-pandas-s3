import pandas as pd
from src.util import func

def run(df:pd.DataFrame) -> pd.DataFrame:
    return (
        df[["vendor"]]
            .join(pd.json_normalize(df.vendor))
            .drop_duplicates(subset=["id"])
            [func.get_table_schema("stg_vendors")]
    )