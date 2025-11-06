import pandas as pd
import duckdb

def normalize_column_names(df) -> pd.DataFrame:
    
    df.columns = (
        df.columns
        .str.strip()
        .str.lower()
        .str.replace(' ' , '_')
        .str.replace(r'[^\w_]' ,'', regex = True)
        )

    print(df.columns)

    return df