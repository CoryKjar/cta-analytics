import sqlite3
import pandas as pd
from src.transform.parse_arrivals import parse_all_arrivals

def load_arrivals_data(df, db_path="data/database/cta.db"):

    conn = sqlite3.connect(db_path)

    df.to_sql('arrivals',
              conn,
              if_exists='append',
              index=False
            )
    
    conn.close()

    print(f"Loaded {len(df)} rows into the arrivals table in the database at {db_path}.")

if __name__ == "__main__":

    df = parse_all_arrivals("data/raw/arrivals")
    load_arrivals_data(df)