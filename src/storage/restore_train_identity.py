"""Restore complete train identities from raw XML, preserving manifest processing history."""
import argparse
import gzip
import hashlib
import json
import math
import os
from collections import Counter, defaultdict
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import pandas as pd
import psycopg
from dotenv import load_dotenv

from src.storage.migrate_train_identity import validate_run_index
from src.storage.object_storage import get_bucket_client
from src.storage.repair_data_quality import load_checkpoint, save_checkpoint
from src.transform.parse_arrivals import parse_arrivals_xml


COLUMNS = (
    "collection_timestamp", "station_id", "stop_id", "route_id", "destination_id",
    "file_id", "direction", "prediction_time", "arrival_time", "is_approaching",
    "is_delayed", "latitude", "longitude", "heading", "wait_time", "run_number",
)
COLUMN_SQL = ",".join(COLUMNS)


def source_records(xml_bytes, file_record, routes, destinations):
    file_id, filename, collected, row_count = file_record
    root = ET.fromstring(xml_bytes)
    if root.findtext("errCd", "0") != "0" or root.findtext("tmst") != collected.strftime("%Y%m%d %H:%M:%S"):
        raise ValueError("Source API timestamp/error does not match the manifest.")
    if filename[:15] != collected.strftime("%Y%m%d_%H%M%S") or "/" in filename or "\\" in filename:
        raise ValueError("Source filename is not a valid manifest basename.")
    frame = parse_arrivals_xml(xml_bytes, filename)
    records = {}
    for row in frame.to_dict("records"):
        if row["route"] not in routes or str(row["destination"]).strip() not in destinations:
            raise ValueError("Source route/destination is absent from the database dimensions.")
        values = (
            collected, int(row["station_id"]), int(row["stop_id"]), routes[row["route"]],
            destinations[str(row["destination"]).strip()], file_id, row["direction"],
            pd.Timestamp(row["prediction_time"]).to_pydatetime(),
            pd.Timestamp(row["arrival_time"]).to_pydatetime(), row["is_approaching"], row["is_delayed"],
            float(row["latitude"]) if pd.notna(row["latitude"]) else None,
            float(row["longitude"]) if pd.notna(row["longitude"]) else None,
            int(row["heading"]) if pd.notna(row["heading"]) else None,
            float(row["wait_time"]), row["run_number"],
        )
        identity = (values[0], values[2], values[3], values[8], values[15])
        if identity in records and records[identity] != values:
            raise ValueError("Same-run source predictions still conflict; refusing to choose one.")
        records[identity] = values
    if not records:
        raise ValueError("A nonempty manifest file has no source predictions.")
    if len({row[1] for row in records.values()}) != 1:
        raise ValueError("Source XML contains more than one station.")
    return list(records.values())


def validate_existing(file_record, existing, expected):
    if len(existing) != file_record[3]:
        raise ValueError("Stored row count differs from the manifest.")
    candidates = defaultdict(list)
    for row in expected:
        candidates[(row[1], row[2], row[3], row[7], row[8])].append(row)
    for old in existing:
        matches = candidates.get((old[1], old[2], old[3], old[7], old[8]), [])
        if old[15] is not None:
            matches = [row for row in matches if row[15] == old[15]]
        def matches_source(row):
            exact = (0, 1, 2, 3, 4, 5, 6, 7, 8, 13)
            return all(old[index] == row[index] for index in exact) and all(
                (old[index] is None and row[index] is None) or
                (old[index] is not None and row[index] is not None and
                 math.isclose(old[index], row[index], rel_tol=0, abs_tol=0.00001))
                for index in (11, 12, 14)
            )
        if not any(matches_source(row) for row in matches):
            raise ValueError("An existing prediction cannot be verified against the source XML.")


def apply_restore(conn, file_record, before, expected, backup_dir):
    validate_existing(file_record, before, expected)
    file_id, filename, collected, row_count = file_record
    with conn.transaction():
        manifest = conn.execute(
            "SELECT file_id,source_file,collection_timestamp,row_count,status,created_at,updated_at,processed_at "
            "FROM ingestion_files WHERE file_id=%s FOR UPDATE", (file_id,),
        ).fetchone()
        if not manifest or tuple(manifest[:4]) != tuple(file_record) or manifest[4] != "processed":
            raise RuntimeError("Manifest changed; restore rolled back.")
        current = conn.execute(
            "SELECT " + COLUMN_SQL + " FROM arrivals WHERE collection_timestamp=%s AND file_id=%s FOR UPDATE",
            (collected, file_id),
        ).fetchall()
        if Counter(current) != Counter(before):
            raise RuntimeError("Predictions changed concurrently; restore rolled back.")
        directory = Path(backup_dir)
        directory.mkdir(parents=True, exist_ok=True)
        path = directory / f"{file_id}_{datetime.now(timezone.utc):%Y%m%dT%H%M%S%fZ}.json.gz"
        with gzip.open(path, "xt", encoding="utf-8") as backup:
            json.dump({"manifest": manifest, "columns": COLUMNS, "before": before, "source": expected}, backup, default=str)
        with conn.cursor() as cursor:
            cursor.execute("CREATE TEMP TABLE train_restore_stage (LIKE arrivals INCLUDING DEFAULTS) ON COMMIT DROP")
            with cursor.copy("COPY train_restore_stage (" + COLUMN_SQL + ") FROM STDIN") as copy:
                for row in expected:
                    copy.write_row(row)
            cursor.execute("DELETE FROM arrivals WHERE collection_timestamp=%s AND file_id=%s", (collected, file_id))
            if cursor.rowcount != len(before):
                raise RuntimeError("Unexpected delete count; restore rolled back.")
            cursor.execute("INSERT INTO arrivals (" + COLUMN_SQL + ") SELECT " + COLUMN_SQL + " FROM train_restore_stage")
            if cursor.rowcount != len(expected):
                raise RuntimeError("Unexpected insert count; restore rolled back.")
            cursor.execute(
                "UPDATE ingestion_files SET row_count=%s,station_id=COALESCE(station_id,%s) WHERE file_id=%s",
                (len(expected), expected[0][1], file_id),
            )
    return path


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--file-id", type=int, action="append", default=[])
    parser.add_argument("--from-checkpoint", help="Restore unresolved IDs listed in a boolean-repair checkpoint")
    parser.add_argument("--public-url", action="store_true")
    parser.add_argument("--apply", action="store_true")
    parser.add_argument("--backup-dir", default="logs/train_identity_backups")
    args = parser.parse_args()
    file_ids = set(args.file_id)
    if args.from_checkpoint:
        with Path(args.from_checkpoint).open(encoding="utf-8") as source:
            saved = json.load(source)
        if saved.get("kind") != "booleans":
            parser.error("Expected a boolean-repair checkpoint.")
        file_ids.update(item["file_id"] for item in saved.get("skipped", []))
    if not file_ids or len(file_ids) > 1000 or any(file_id <= 0 for file_id in file_ids):
        parser.error("Select 1..1000 positive file IDs.")
    load_dotenv()
    url = os.getenv("DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL")
    if not url or not os.getenv("BUCKET"):
        parser.error("The selected database URL and BUCKET must be configured.")
    mode = "off" if args.apply else "on"
    with psycopg.connect(url, autocommit=True, connect_timeout=15,
                         options=f"-c default_transaction_read_only={mode} -c statement_timeout=30000 -c lock_timeout=2000") as conn:
        if args.from_checkpoint:
            database = hashlib.sha256(f"{conn.info.host}:{conn.info.port}/{conn.info.dbname}".encode()).hexdigest()
            load_checkpoint(args.from_checkpoint, database)
        validate_run_index(conn)
        routes = {name: route_id for route_id, name in conn.execute("SELECT route_id,route_name FROM routes").fetchall()}
        destinations = {name: dest_id for dest_id, name in conn.execute("SELECT destination_id,destination_name FROM destinations").fetchall()}
        client = get_bucket_client()
        print("Mode:", "APPLY" if args.apply else "READ-ONLY")
        for file_id in sorted(file_ids):
            record = conn.execute(
                "SELECT file_id,source_file,collection_timestamp,row_count FROM ingestion_files WHERE file_id=%s AND status='processed'",
                (file_id,),
            ).fetchone()
            if not record:
                raise ValueError("Requested file is missing or not processed.")
            response = client.get_object(Bucket=os.environ["BUCKET"], Key=record[2].strftime("raw/%Y/%m/%d/") + record[1])
            try:
                expected = source_records(response["Body"].read(), record, routes, destinations)
            finally:
                response["Body"].close()
            before = conn.execute(
                "SELECT " + COLUMN_SQL + " FROM arrivals WHERE collection_timestamp=%s AND file_id=%s",
                (record[2], file_id),
            ).fetchall()
            validate_existing(record, before, expected)
            backup = apply_restore(conn, record, before, expected, args.backup_dir) if args.apply else None
            print(f"file_id={file_id} | old_rows={len(before)} | source_rows={len(expected)} | restored={args.apply} | backup={backup}", flush=True)
        if args.apply and args.from_checkpoint:
            with Path(args.from_checkpoint).open(encoding="utf-8") as source:
                current = json.load(source)
            if current != saved:
                print("Restores committed; checkpoint changed concurrently and was not modified.")
            else:
                remaining = [item for item in current.get("skipped", []) if item["file_id"] not in file_ids]
                save_checkpoint(args.from_checkpoint, current["after"], current["cutoff"], current["database"], remaining)
                print("Unresolved checkpoint files remaining:", len(remaining))


if __name__ == "__main__":
    main()