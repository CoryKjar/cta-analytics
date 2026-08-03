import sqlite3
import pandas as pd
from src.transform.parse_arrivals import parse_all_arrivals

def load_arrivals_data(df, db_path="data/database/cta.db"):

    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()

    cursor.execute("""
                   CREATE TABLE IF NOT EXISTS arrivals (
                        collection_timestamp TEXT,
                        station_id TEXT,
                        station TEXT,
                        stop_id TEXT,
                        route TEXT,
                        destination TEXT,
                        direction TEXT,
                        prediction_time TEXT,
                        arrival_time TEXT,
                        is_approaching INTEGER,
                        is_delayed INTEGER,
                        latitude REAL,
                        longitude REAL,
                        heading INTEGER,
                        wait_time REAL,
                        source_file TEXT,

                        UNIQUE (
                            collection_timestamp,
                            stop_id,
                            route,
                            arrival_time
                   )
                )
                """)
    
    inserted = 0
    duplicates = 0
    
    for _, row in df.iterrows():
        values = (
            row['collection_timestamp'].isoformat(),
            row['station_id'],
            row['station'],
            row['stop_id'],
            row['route'],
            row['destination'],
            row['direction'],
            row['prediction_time'].isoformat(),
            row['arrival_time'].isoformat(),
            int(row['is_approaching']),
            int(row['is_delayed']),
            float(row["latitude"]) if pd.notna(row["latitude"]) else None,
            float(row["longitude"]) if pd.notna(row["longitude"]) else None,
            int(row["heading"]) if pd.notna(row["heading"]) else None,
            float(row['wait_time']),
            row['source_file']
        )

        cursor.execute("""
            INSERT OR IGNORE INTO arrivals (
                collection_timestamp,
                station_id,
                station,
                stop_id,
                route,
                destination,
                direction,
                prediction_time,
                arrival_time,
                is_approaching,
                is_delayed,
                latitude,
                longitude,
                heading,
                wait_time,
                source_file
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """, values)

        if cursor.rowcount == 1:
            inserted += 1
        else:
            duplicates += 1

    conn.commit()
    conn.close()


    print(f"Inserted:  {inserted}")
    print(f"Duplicates skipped: {duplicates}")

if __name__ == "__main__":

    df = parse_all_arrivals("data/raw/arrivals")
    load_arrivals_data(df)