import os
import pandas as pd
import psycopg
from dotenv import load_dotenv


def load_arrivals_data(df):

    load_dotenv()

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables.")

    with psycopg.connect(database_url) as conn:

        with conn.cursor() as cursor:

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

            records = []

            for _, row in df.iterrows():

                records.append((
                    row["collection_timestamp"].isoformat(),
                    row["station_id"],
                    row["station"],
                    row["stop_id"],
                    row["route"],
                    row["destination"],
                    row["direction"],
                    row["prediction_time"].isoformat(),
                    row["arrival_time"].isoformat(),
                    int(row["is_approaching"]),
                    int(row["is_delayed"]),
                    float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                    float(row["longitude"]) if pd.notna(row["longitude"]) else None,
                    int(row["heading"]) if pd.notna(row["heading"]) else None,
                    float(row["wait_time"]),
                    row["source_file"]
                ))

            query = """
                INSERT INTO arrivals (
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
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    collection_timestamp,
                    stop_id,
                    route,
                    arrival_time
                )
                DO NOTHING
            """

            cursor.executemany(query, records)

            print(f"Processed: {len(records)} rows")

        conn.commit()

    print("PostgreSQL load complete.")


if __name__ == "__main__":

    from src.transform.parse_arrivals import parse_all_arrivals

    df, processed_files = parse_all_arrivals("data/raw/arrivals")

    load_arrivals_data(df)