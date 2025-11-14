import pandas as pd
from src.utils.utils import normalize_column_names
from src.utils.db import get_connection
from src.config import configuration
import duckdb


def extract(file_name: str) -> pd.DataFrame: # simple extract function
    return pd.read_csv(f"data/{file_name}", low_memory=False)


def transform(df):
    #Normalizimi i emrave te kolonave --Column name normalization
    df = normalize_column_names(df)

    #Konvertimi i dates -- Date Conversion
    df["expiration_date"] = pd.to_datetime(df["expiration_date"], format="%m/%d/%Y")
  
    #Heq hapesirat e paneojshme ne kolona -- Removes unnecessary spaces in columns
    for col in df.select_dtypes(include='object').columns:
        df[col] = df[col].str.strip()
    
    #Heq rreshat qe gjinden nga dy her -- Remove duplicate rows
    df = df.drop_duplicates(subset=['vehicle_license_number', 'dmv_license_plate_number'])
    
    #Mban vetem kto kolona -- Retain only these Columns
    keep_cols = [
    'vehicle_license_number',
    'license_type',
    'dmv_license_plate_number',
    'vehicle_vin_number',
    'expiration_date',
    'wheelchair_accessible',
    'active'
    ]
    df = df[keep_cols]

    #Transformimi me Pandas -- Transformation with Pandas
    df = df.sort_values(by="expiration_date", ascending=True)
    
    #Transformimi me duckdb -- Transformation with Duckdb
    # con = duckdb.connect()
    # con.register("vhc", df)

    
    # df = con.execute("""
    # SELECT 
    # --expiration_date
    # --expiration_date, license_type
    # *
    # --*, (expiration_date::DATE - CURRENT_DATE)::INT AS days_until_expiration
    # FROM vhc
    # ORDER BY expiration_date
    
    # """).fetchdf()

    print(df)

    return df


def load(df: pd.DataFrame, engine) -> pd.DataFrame: # simple load function
    df.to_sql("fhv_active_cleaned", engine, if_exists="replace", index=False)

    