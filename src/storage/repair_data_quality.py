"""Bounded, dry-run-by-default station metadata and source-backed boolean repair."""
import argparse
import os
from datetime import datetime
import xml.etree.ElementTree as ET

import psycopg
from dotenv import load_dotenv

from src.config.stations import station_id_from_filename
from src.storage.object_storage import get_bucket_client
from src.transform.parse_arrivals import parse_cta_boolean


STATION_REPAIR_SQL = """
    UPDATE ingestion_files SET station_id = %s
    WHERE file_id = %s AND station_id IS NULL
      AND status = 'processed' AND row_count = 0
"""

FLAG_REPAIR_SQL = """
    UPDATE arrivals SET is_approaching = %s, is_delayed = %s
    WHERE file_id = %s AND collection_timestamp = %s
      AND station_id = %s AND stop_id = %s AND route_id = %s
      AND prediction_time = %s AND arrival_time = %s
      AND is_approaching IS NOT DISTINCT FROM %s
      AND is_delayed IS NOT DISTINCT FROM %s
"""


def plan_station_repairs(files, valid_station_ids):
    """Only map exact known filenames whose station exists in this database."""
    repairs, unresolved = [], []
    for file_id, source_file, collected, row_count in files:
        station_id = station_id_from_filename(source_file)
        if station_id is None or station_id not in valid_station_ids:
            unresolved.append(source_file)
        else:
            repairs.append((station_id, file_id))
    return repairs, unresolved


def source_boolean_flags(xml_bytes, collected, route_ids):
    """Key original API flags by the complete stored prediction identity."""
    root = ET.fromstring(xml_bytes)
    if root.findtext("errCd", "0") != "0":
        raise ValueError("Source XML contains a CTA API error.")
    if root.findtext("tmst") != collected.strftime("%Y%m%d %H:%M:%S"):
        raise ValueError("Source API timestamp does not match the manifest.")
    flags = {}
    for eta in root.findall("eta"):
        route = eta.findtext("rt")
        if route not in route_ids:
            raise ValueError("Source XML contains an unknown route.")
        identity = (
            int(eta.findtext("staId")), int(eta.findtext("stpId")), route_ids[route],
            datetime.strptime(eta.findtext("prdt"), "%Y%m%d %H:%M:%S"),
            datetime.strptime(eta.findtext("arrT"), "%Y%m%d %H:%M:%S"),
        )
        values = (parse_cta_boolean(eta.findtext("isApp")), parse_cta_boolean(eta.findtext("isDly")))
        if identity in flags and flags[identity] != values:
            raise ValueError("Source XML contains conflicting duplicate flags.")
        flags[identity] = values
    return flags


def plan_boolean_repairs(file_record, stored_rows, source_flags):
    file_id, source_file, collected, row_count = file_record
    if len(stored_rows) != row_count:
        raise ValueError("Manifest row count differs from stored rows; refusing repair.")
    repairs = []
    for station_id, stop_id, route_id, prediction, arrival, approaching, delayed in stored_rows:
        identity = (station_id, stop_id, route_id, prediction, arrival)
        if identity not in source_flags:
            raise ValueError("A stored prediction has no exact source XML match.")
        expected = source_flags[identity]
        if expected != (approaching, delayed):
            repairs.append((
                *expected, file_id, collected, *identity, approaching, delayed,
            ))
    return repairs


def apply_repairs(conn, statement, repairs):
    """Apply atomically and reject a concurrent change rather than overwrite it."""
    if not repairs:
        return
    with conn.transaction():
        with conn.cursor() as cursor:
            cursor.executemany(statement, repairs)
            if cursor.rowcount != len(repairs):
                raise RuntimeError("Rows changed concurrently; this repair transaction was rolled back.")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=["station-ids", "booleans"], required=True)
    parser.add_argument("--limit", type=int, default=25, help="Maximum files; default 25")
    parser.add_argument("--after-file-id", type=int, default=0, help="Resume after the reported file ID")
    parser.add_argument("--public-url", action="store_true", help="Use DATABASE_PUBLIC_URL from Railway")
    parser.add_argument("--apply", action="store_true", help="Explicitly authorize database updates")
    args = parser.parse_args()
    maximum = 1000 if args.kind == "booleans" else 50000
    if not 1 <= args.limit <= maximum or args.after_file_id < 0:
        parser.error(f"Limit must be 1..{maximum}; after-file-id must be non-negative.")
    load_dotenv()
    variable = "DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL"
    url = os.getenv(variable)
    if not url:
        parser.error(f"{variable} is not configured.")
    mode = "off" if args.apply else "on"
    with psycopg.connect(
        url, connect_timeout=15, autocommit=True,
        options=f"-c default_transaction_read_only={mode} -c statement_timeout=30000 -c lock_timeout=2000",
    ) as conn:
        condition = "station_id IS NULL AND row_count=0" if args.kind == "station-ids" else "row_count>0"
        files = conn.execute(
            "SELECT file_id,source_file,collection_timestamp,row_count FROM ingestion_files "
            "WHERE status='processed' AND file_id>%s AND " + condition + " ORDER BY file_id LIMIT %s",
            (args.after_file_id, args.limit),
        ).fetchall()
        print(f"Mode: {'APPLY' if args.apply else 'DRY RUN (read-only)'} | Kind: {args.kind} | Files: {len(files)}")
        if args.kind == "station-ids":
            valid_ids = {row[0] for row in conn.execute("SELECT station_id FROM stations").fetchall()}
            repairs, unresolved = plan_station_repairs(files, valid_ids)
            print(f"Resolvable: {len(repairs)} | Unresolved: {len(unresolved)}")
            if unresolved:
                raise ValueError("Unresolved station filenames; no station repairs were applied.")
            if args.apply:
                apply_repairs(conn, STATION_REPAIR_SQL, repairs)
            print(f"Rows {'updated' if args.apply else 'that would change'}: {len(repairs)}")
        else:
            bucket = os.getenv("BUCKET")
            if not bucket:
                parser.error("BUCKET is not configured.")
            client = get_bucket_client()
            routes = {name: route_id for route_id, name in conn.execute("SELECT route_id,route_name FROM routes").fetchall()}
            changed = 0
            for record in files:
                file_id, source_file, collected, row_count = record
                if source_file[:15] != collected.strftime("%Y%m%d_%H%M%S") or "/" in source_file or "\\" in source_file:
                    raise ValueError("Source filename does not match the collection timestamp.")
                key = collected.strftime("raw/%Y/%m/%d/") + source_file
                response = client.get_object(Bucket=bucket, Key=key)
                try:
                    flags = source_boolean_flags(response["Body"].read(), collected, routes)
                finally:
                    response["Body"].close()
                stored = conn.execute(
                    "SELECT station_id,stop_id,route_id,prediction_time,arrival_time,is_approaching,is_delayed "
                    "FROM arrivals WHERE collection_timestamp=%s AND file_id=%s",
                    (collected, file_id),
                ).fetchall()
                repairs = plan_boolean_repairs(record, stored, flags)
                if args.apply:
                    apply_repairs(conn, FLAG_REPAIR_SQL, repairs)
                changed += len(repairs)
                print(f"file_id={file_id} | rows={len(stored)} | corrections={len(repairs)}")
            print(f"Rows {'updated' if args.apply else 'that would change'}: {changed}")
        if files:
            print(f"Next batch: --after-file-id {files[-1][0]}")


if __name__ == "__main__":
    main()