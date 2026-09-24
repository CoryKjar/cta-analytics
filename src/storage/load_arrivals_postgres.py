import os

import pandas as pd
import psycopg
from dotenv import load_dotenv


def load_arrivals_data(df):
    load_dotenv()

    database_url = os.getenv("DATABASE_URL")

    if not database_url:
        raise ValueError("DATABASE_URL not found in environment variables.")

    if df.empty:
        print("No arrival data to load.")
        return

    with psycopg.connect(database_url) as conn:
        with conn.cursor() as cursor:

            # ---------------------------------------------------------
            # 1. Load route mappings
            # ---------------------------------------------------------
            cursor.execute("""
                SELECT route_id, route_name
                FROM routes;
            """)

            route_map = {
                route_name: route_id
                for route_id, route_name in cursor.fetchall()
            }

            # ---------------------------------------------------------
            # 2. Load destination mappings
            # ---------------------------------------------------------
            cursor.execute("""
                SELECT destination_id, destination_name
                FROM destinations;
            """)

            destination_map = {
                destination_name: destination_id
                for destination_id, destination_name in cursor.fetchall()
            }

            # ---------------------------------------------------------
            # 3. Validate that all incoming dimension values exist
            # ---------------------------------------------------------
            incoming_routes = set(df["route"].dropna().unique())
            missing_routes = incoming_routes - set(route_map)

            if missing_routes:
                raise ValueError(
                    f"Unknown routes found in incoming data: {missing_routes}"
                )

            incoming_destinations = set(
                df["destination"].dropna().unique()
            )
            missing_destinations = (
                incoming_destinations - set(destination_map)
            )

            if missing_destinations:
                raise ValueError(
                    "Unknown destinations found in incoming data: "
                    f"{missing_destinations}"
                )

            # ---------------------------------------------------------
            # 4. Register source files
            # ---------------------------------------------------------
            source_files = (
                df[
                    [
                        "source_file",
                        "collection_timestamp",
                        "station_id",
                    ]
                ]
                .drop_duplicates(subset=["source_file"])
            )

            file_records = []

            for _, row in source_files.iterrows():
                file_records.append(
                    (
                        str(row["source_file"]),
                        pd.Timestamp(
                            row["collection_timestamp"]
                        ).to_pydatetime(),
                        int(row["station_id"]),
                    )
                )

            cursor.executemany(
                """
                INSERT INTO ingestion_files (
                    source_file,
                    collection_timestamp,
                    station_id
                )
                VALUES (%s, %s, %s)
                ON CONFLICT (source_file)
                DO NOTHING;
                """,
                file_records,
            )

            # ---------------------------------------------------------
            # 5. Retrieve file_id mappings
            # ---------------------------------------------------------
            source_file_names = [
                record[0]
                for record in file_records
            ]

            cursor.execute(
                """
                SELECT file_id, source_file
                FROM ingestion_files
                WHERE source_file = ANY(%s);
                """,
                (source_file_names,),
            )

            file_map = {
                source_file: file_id
                for file_id, source_file in cursor.fetchall()
            }

            # ---------------------------------------------------------
            # 6. Build compact arrival records
            # ---------------------------------------------------------
            records = []

            for _, row in df.iterrows():

                source_file = str(row["source_file"])

                if source_file not in file_map:
                    raise ValueError(
                        f"Could not find file_id for {source_file}"
                    )

                records.append(
                    (
                        pd.Timestamp(
                            row["collection_timestamp"]
                        ).to_pydatetime(),

                        int(row["station_id"]),
                        int(row["stop_id"]),

                        route_map[row["route"]],
                        destination_map[row["destination"]],

                        file_map[source_file],

                        row["direction"],

                        pd.Timestamp(
                            row["prediction_time"]
                        ).to_pydatetime(),

                        pd.Timestamp(
                            row["arrival_time"]
                        ).to_pydatetime(),

                        (
                            bool(row["is_approaching"])
                            if pd.notna(row["is_approaching"])
                            else None
                        ),

                        (
                            bool(row["is_delayed"])
                            if pd.notna(row["is_delayed"])
                            else None
                        ),

                        (
                            float(row["latitude"])
                            if pd.notna(row["latitude"])
                            else None
                        ),

                        (
                            float(row["longitude"])
                            if pd.notna(row["longitude"])
                            else None
                        ),

                        (
                            int(row["heading"])
                            if pd.notna(row["heading"])
                            else None
                        ),

                        (
                            float(row["wait_time"])
                            if pd.notna(row["wait_time"])
                            else None
                        ),
                    )
                )

            # ---------------------------------------------------------
            # 7. Insert arrivals
            # ---------------------------------------------------------
            query = """
                INSERT INTO arrivals (
                    collection_timestamp,
                    station_id,
                    stop_id,
                    route_id,
                    destination_id,
                    file_id,
                    direction,
                    prediction_time,
                    arrival_time,
                    is_approaching,
                    is_delayed,
                    latitude,
                    longitude,
                    heading,
                    wait_time
                )
                VALUES (
                    %s, %s, %s, %s, %s, %s, %s,
                    %s, %s, %s, %s, %s, %s, %s, %s
                )
                ON CONFLICT (
                    collection_timestamp,
                    stop_id,
                    route_id,
                    arrival_time
                )
                DO NOTHING;
            """

            cursor.executemany(query, records)

        conn.commit()

    print(f"PostgreSQL load complete: {len(records)} rows processed.")