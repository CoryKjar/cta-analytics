import os
import re
from datetime import datetime
from pathlib import Path

import boto3
import pandas as pd
import psycopg
from botocore.config import Config
from dotenv import load_dotenv

from src.transform.parse_arrivals import parse_arrivals
from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results

load_dotenv()
STAGING_DIR = Path("data/raw/arrivals")
PREFIX = "raw/"
BATCH_SIZE = 250


def timestamp_from_filename(name):
    match = re.match(r"^(\d{8})_(\d{6})_.+\.xml$", name)
    if not match:
        raise ValueError(f"Unexpected filename format: {name}")
    return datetime.strptime(
        f"{match.group(1)}_{match.group(2)}", "%Y%m%d_%H%M%S"
    )


def main():
    database_url = os.getenv("DATABASE_URL")
    bucket = os.getenv("BUCKET")
    if not database_url or not bucket:
        raise ValueError("DATABASE_URL and BUCKET must be configured.")

    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION", "auto"),
        config=Config(s3={"addressing_style": "virtual"}),
    )
    STAGING_DIR.mkdir(parents=True, exist_ok=True)

    print("Reading manifest...", flush=True)
    with psycopg.connect(database_url, connect_timeout=15) as conn:
        statuses = dict(conn.execute(
            "SELECT source_file, status FROM ingestion_files"
        ).fetchall())

    print("Listing Bucket objects (may take a few minutes)...", flush=True)
    objects = {}
    for page in client.get_paginator("list_objects_v2").paginate(
        Bucket=bucket, Prefix=PREFIX
    ):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.endswith(".xml"):
                objects[key.rsplit("/", 1)[-1]] = key

    candidates = sorted(
        name for name in objects if statuses.get(name) != "processed"
    )
    print(f"Bucket XML: {len(objects):,}; candidates: {len(candidates):,}", flush=True)
    if not candidates:
        print("Nothing to replay.")
        return

    stats = {"loaded_files": 0, "empty_files": 0, "parse_failures": 0,
             "load_failures": 0, "download_failures": 0}

    # Download, parse and commit each bounded batch before moving to the next.
    for start in range(0, len(candidates), BATCH_SIZE):
        names = candidates[start:start + BATCH_SIZE]
        print(
            f"\nBatch {start // BATCH_SIZE + 1}: "
            f"{start + 1:,}-{start + len(names):,} of {len(candidates):,}",
            flush=True,
        )
        frames, empty, failures = [], [], []

        for name in names:
            path = STAGING_DIR / name
            if not path.exists():
                try:
                    client.download_file(bucket, objects[name], str(path))
                except Exception as exc:
                    stats["download_failures"] += 1
                    failures.append((name, str(exc)))
                    print(f"DOWNLOAD ERROR {name}: {exc}")
                    continue
            try:
                timestamp = timestamp_from_filename(name)
                df = parse_arrivals(path)
                if df.empty:
                    empty.append({
                        "source_file": name,
                        "collection_timestamp": timestamp,
                        "status": "processed",
                        "row_count": 0,
                        "station_id": None,
                        "last_error": None,
                    })
                else:
                    frames.append(df)
            except Exception as exc:
                stats["parse_failures"] += 1
                failures.append((name, str(exc)))
                print(f"PARSE ERROR {name}: {exc}")

        # One database transaction for all empty files in this batch.
        if empty:
            try:
                register_file_results(empty)
                stats["empty_files"] += len(empty)
            except Exception as exc:
                print(f"EMPTY-FILE BULK REGISTRATION FAILED: {exc}")
                for item in empty:
                    item["status"] = "failed"
                    item["last_error"] = str(exc)[:4000]
                try:
                    register_file_results(empty)
                except Exception as retry_exc:
                    print(f"Could not record empty-file failures: {retry_exc}")

        if failures:
            failed_records = []
            for name, error in failures:
                try:
                    ts = timestamp_from_filename(name)
                except ValueError:
                    continue
                failed_records.append({
                    "source_file": name,
                    "collection_timestamp": ts,
                    "status": "failed",
                    "row_count": 0,
                    "station_id": None,
                    "last_error": error[:4000],
                })
            if failed_records:
                try:
                    register_file_results(failed_records)
                except Exception as exc:
                    print(f"Could not bulk-record failures: {exc}")

        if frames:
            df = pd.concat(frames, ignore_index=True)
            count = df["source_file"].nunique()
            try:
                load_arrivals_data(df)
                stats["loaded_files"] += count
            except Exception as exc:
                stats["load_failures"] += count
                print(f"BATCH LOAD FAILED: {exc}")
                failed_records = []
                for name in df["source_file"].drop_duplicates():
                    row = df[df["source_file"] == name].iloc[0]
                    failed_records.append({
                        "source_file": str(name),
                        "collection_timestamp": pd.Timestamp(
                            row["collection_timestamp"]
                        ).to_pydatetime(),
                        "status": "failed",
                        "row_count": 0,
                        "station_id": int(row["station_id"]),
                        "last_error": str(exc)[:4000],
                    })
                try:
                    register_file_results(failed_records)
                except Exception as status_exc:
                    print(f"Could not record batch failures: {status_exc}")

        done = start + len(names)
        print(
            f"Progress {done:,}/{len(candidates):,} | "
            f"loaded files {stats['loaded_files']:,} | "
            f"empty {stats['empty_files']:,} | "
            f"parse failures {stats['parse_failures']:,} | "
            f"load failures {stats['load_failures']:,} | "
            f"download failures {stats['download_failures']:,}",
            flush=True,
        )

    print("\nReplay summary:")
    for key, value in stats.items():
        print(f"{key}: {value:,}")
    print("Local XML files retained.")


if __name__ == "__main__":
    main()
