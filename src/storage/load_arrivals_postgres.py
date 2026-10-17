import os

import pandas as pd
import psycopg
from dotenv import load_dotenv

from src.config.stations import station_id_from_filename
from src.transform.parse_arrivals import parse_cta_boolean


def _database_url():
    load_dotenv()
    value = os.getenv("DATABASE_URL")
    if not value:
        raise ValueError("DATABASE_URL not found in environment variables.")
    return value


def register_file_results(results):
    """Bulk-register file outcomes in one transaction."""
    if not results:
        return

    allowed = {"pending", "processing", "processed", "failed"}
    for item in results:
        if item["status"] not in allowed:
            raise ValueError(f"Invalid ingestion status: {item['status']}")

    records = [
        (
            item["source_file"],
            item["collection_timestamp"],
            item["station_id"] if item.get("station_id") is not None
            else station_id_from_filename(item["source_file"]),
            item["status"],
            int(item.get("row_count", 0)),
            item["status"],
            item.get("last_error"),
            item["status"],
        )
        for item in results
    ]

    with psycopg.connect(_database_url(), connect_timeout=15) as conn:
        with conn.cursor() as cursor:
            cursor.executemany(
                """
                INSERT INTO ingestion_files (
                    source_file, collection_timestamp, station_id, status,
                    row_count, attempts, last_error, created_at, updated_at,
                    processed_at
                )
                VALUES (
                    %s, %s, %s, %s, %s,
                    CASE WHEN %s = 'failed' THEN 1 ELSE 0 END,
                    %s, NOW(), NOW(),
                    CASE WHEN %s = 'processed' THEN NOW() ELSE NULL END
                )
                ON CONFLICT (source_file) DO UPDATE SET
                    collection_timestamp = EXCLUDED.collection_timestamp,
                    station_id = COALESCE(
                        EXCLUDED.station_id, ingestion_files.station_id
                    ),
                    status = CASE
                        WHEN EXCLUDED.row_count = 0
                         AND EXISTS (
                             SELECT 1
                             FROM arrivals a
                             WHERE a.file_id = ingestion_files.file_id
                         )
                        THEN 'processed'
                        ELSE EXCLUDED.status
                    END,
                    row_count = CASE
                        WHEN EXCLUDED.row_count = 0
                         AND EXISTS (
                             SELECT 1
                             FROM arrivals a
                             WHERE a.file_id = ingestion_files.file_id
                         )
                        THEN (
                            SELECT COUNT(*)::INTEGER
                            FROM arrivals a
                            WHERE a.file_id = ingestion_files.file_id
                        )
                        ELSE EXCLUDED.row_count
                    END,
                    attempts = ingestion_files.attempts
                        + CASE WHEN EXCLUDED.status = 'failed' THEN 1 ELSE 0 END,
                    last_error = CASE
                        WHEN EXCLUDED.row_count = 0
                         AND EXISTS (
                             SELECT 1
                             FROM arrivals a
                             WHERE a.file_id = ingestion_files.file_id
                         )
                        THEN NULL
                        ELSE EXCLUDED.last_error
                    END,
                    updated_at = NOW(),
                    processed_at = CASE
                        WHEN EXCLUDED.row_count = 0
                         AND EXISTS (
                             SELECT 1
                             FROM arrivals a
                             WHERE a.file_id = ingestion_files.file_id
                         )
                        THEN COALESCE(ingestion_files.processed_at, NOW())
                        ELSE EXCLUDED.processed_at
                    END;
                """,
                records,
            )


def register_file_result(
    source_file, collection_timestamp, *, status, row_count=0,
    station_id=None, last_error=None
):
    """Compatibility wrapper for a single file."""
    register_file_results([{
        "source_file": source_file,
        "collection_timestamp": collection_timestamp,
        "status": status,
        "row_count": row_count,
        "station_id": station_id,
        "last_error": last_error,
    }])


def load_arrivals_data(df):
    """Load a non-empty DataFrame and update manifest rows in the same transaction."""
    if df.empty:
        return

    required = {
        "source_file", "collection_timestamp", "station_id", "stop_id",
        "route", "destination", "direction", "prediction_time",
        "arrival_time", "is_approaching", "is_delayed", "latitude",
        "longitude", "heading", "wait_time",
    }
    missing = required - set(df.columns)
    if missing:
        raise ValueError(f"Arrival DataFrame missing columns: {sorted(missing)}")

    df = df.copy()
    for column in ("is_approaching", "is_delayed"):
        df[column] = df[column].map(parse_cta_boolean)

    with psycopg.connect(_database_url(), connect_timeout=15) as conn:
        with conn.cursor() as cursor:
            cursor.execute("SELECT route_id, route_name FROM routes")
            route_map = {name: route_id for route_id, name in cursor.fetchall()}
            missing_routes = set(df["route"].dropna().unique()) - set(route_map)
            if missing_routes:
                raise ValueError(f"Unknown routes: {missing_routes}")

            incoming_destinations = {
                str(v).strip() for v in df["destination"].dropna().unique()
                if str(v).strip()
            }
            cursor.execute("LOCK TABLE destinations IN EXCLUSIVE MODE")
            cursor.execute("SELECT destination_id, destination_name FROM destinations")
            destination_map = {name: dest_id for dest_id, name in cursor.fetchall()}
            missing_destinations = incoming_destinations - set(destination_map)

            if missing_destinations:
                cursor.execute("SELECT COALESCE(MAX(destination_id), 0) FROM destinations")
                max_id = cursor.fetchone()[0]
                for offset, name in enumerate(sorted(missing_destinations), start=1):
                    cursor.execute(
                        "INSERT INTO destinations (destination_id, destination_name) VALUES (%s, %s)",
                        (max_id + offset, name),
                    )
                cursor.execute("SELECT destination_id, destination_name FROM destinations")
                destination_map = {name: dest_id for dest_id, name in cursor.fetchall()}

            source_files = df[
                ["source_file", "collection_timestamp", "station_id"]
            ].drop_duplicates(subset=["source_file"])
            file_records = [
                (
                    str(row["source_file"]),
                    pd.Timestamp(row["collection_timestamp"]).to_pydatetime(),
                    int(row["station_id"]),
                )
                for _, row in source_files.iterrows()
            ]

            cursor.executemany(
                """
                INSERT INTO ingestion_files (
                    source_file, collection_timestamp, station_id,
                    status, row_count, attempts, created_at, updated_at
                )
                VALUES (%s, %s, %s, 'processing', 0, 0, NOW(), NOW())
                ON CONFLICT (source_file) DO UPDATE SET
                    collection_timestamp = EXCLUDED.collection_timestamp,
                    station_id = EXCLUDED.station_id,
                    status = 'processing',
                    last_error = NULL,
                    updated_at = NOW();
                """,
                file_records,
            )

            names = [record[0] for record in file_records]
            cursor.execute(
                "SELECT file_id, source_file FROM ingestion_files WHERE source_file = ANY(%s)",
                (names,),
            )
            file_map = {name: file_id for file_id, name in cursor.fetchall()}

            records = []
            for _, row in df.iterrows():
                records.append(
                    (
                        pd.Timestamp(row["collection_timestamp"]).to_pydatetime(),
                        int(row["station_id"]),
                        int(row["stop_id"]),
                        route_map[row["route"]],
                        destination_map[str(row["destination"]).strip()],
                        file_map[str(row["source_file"])],
                        row["direction"],
                        pd.Timestamp(row["prediction_time"]).to_pydatetime(),
                        pd.Timestamp(row["arrival_time"]).to_pydatetime(),
                        parse_cta_boolean(row["is_approaching"]),
                        parse_cta_boolean(row["is_delayed"]),
                        float(row["latitude"]) if pd.notna(row["latitude"]) else None,
                        float(row["longitude"]) if pd.notna(row["longitude"]) else None,
                        int(row["heading"]) if pd.notna(row["heading"]) else None,
                        float(row["wait_time"]) if pd.notna(row["wait_time"]) else None,
                    )
                )

            cursor.executemany(
                """
                INSERT INTO arrivals (
                    collection_timestamp, station_id, stop_id, route_id,
                    destination_id, file_id, direction, prediction_time,
                    arrival_time, is_approaching, is_delayed, latitude,
                    longitude, heading, wait_time
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (
                    collection_timestamp, stop_id, route_id, arrival_time
                ) DO UPDATE SET
                    is_approaching = EXCLUDED.is_approaching,
                    is_delayed = EXCLUDED.is_delayed
                WHERE arrivals.file_id = EXCLUDED.file_id
                  AND (
                      arrivals.is_approaching IS DISTINCT FROM EXCLUDED.is_approaching
                      OR arrivals.is_delayed IS DISTINCT FROM EXCLUDED.is_delayed
                  )
                """,
                records,
            )

            cursor.execute(
                """
                UPDATE ingestion_files AS f
                SET row_count = counts.row_count,
                    status = 'processed',
                    last_error = NULL,
                    processed_at = NOW(),
                    updated_at = NOW()
                FROM (
                    SELECT f2.file_id, COUNT(a.file_id)::INTEGER AS row_count
                    FROM ingestion_files f2
                    LEFT JOIN arrivals a ON a.file_id = f2.file_id
                    WHERE f2.source_file = ANY(%s)
                    GROUP BY f2.file_id
                ) counts
                WHERE f.file_id = counts.file_id
                """,
                (names,),
            )

            print(
    "Manifest completion update:",
    cursor.rowcount,
    "rows updated for",
    len(names),
    "source files",
)

    print(f"Loaded {len(records):,} parsed arrival rows from {len(file_records):,} files.")
