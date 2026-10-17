"""Bounded, dry-run-by-default station metadata and source-backed boolean repair."""
import argparse
import hashlib
import json
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
from pathlib import Path
from time import monotonic
import xml.etree.ElementTree as ET

import psycopg
from dotenv import load_dotenv

from src.config.stations import station_id_from_filename
from src.storage.object_storage import get_bucket_client
from src.transform.parse_arrivals import parse_cta_boolean, parse_cta_run_number


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
RUN_FLAG_REPAIR_SQL = FLAG_REPAIR_SQL + " AND run_number IS NOT DISTINCT FROM %s"


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


def source_boolean_flags(xml_bytes, collected, route_ids, *, include_runs=False):
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
        if include_runs:
            identity += (parse_cta_run_number(eta.findtext("rn")),)
        values = (parse_cta_boolean(eta.findtext("isApp")), parse_cta_boolean(eta.findtext("isDly")))
        if identity in flags and flags[identity] != values:
            raise ValueError("Source XML contains conflicting duplicate flags.")
        flags[identity] = values
    return flags


def plan_boolean_repairs(file_record, stored_rows, source_flags, *, include_runs=False):
    file_id, source_file, collected, row_count = file_record
    if len(stored_rows) != row_count:
        raise ValueError("Manifest row count differs from stored rows; refusing repair.")
    repairs = []
    for stored in stored_rows:
        station_id, stop_id, route_id, prediction, arrival, approaching, delayed = stored[:7]
        identity = (station_id, stop_id, route_id, prediction, arrival)
        if include_runs and stored[7] is None:
            matches = [value for key, value in source_flags.items() if key[:-1] == identity]
            if len(matches) != 1:
                raise ValueError("Legacy train identity needs source-backed train restoration.")
            expected = matches[0]
        else:
            key = identity + (stored[7],) if include_runs else identity
            if key not in source_flags:
                raise ValueError("A stored prediction has no exact source XML match.")
            expected = source_flags[key]
        if expected != (approaching, delayed):
            parameters = (*expected, file_id, collected, *identity, approaching, delayed)
            repairs.append(parameters + (stored[7],) if include_runs else parameters)
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


def read_source_flags(client, bucket, record, routes, *, include_runs=False):
    file_id, source_file, collected, row_count = record
    if source_file[:15] != collected.strftime("%Y%m%d_%H%M%S") or "/" in source_file or "\\" in source_file:
        raise ValueError("Source filename does not match the collection timestamp.")
    response = client.get_object(
        Bucket=bucket, Key=collected.strftime("raw/%Y/%m/%d/") + source_file,
    )
    try:
        return source_boolean_flags(response["Body"].read(), collected, routes, include_runs=include_runs)
    finally:
        response["Body"].close()


def plan_boolean_batch(conn, client, bucket, files, routes, workers, invalid_files=None, *, include_runs=False):
    """Validate every source before applying an atomic batch of corrections."""
    if not files:
        return []
    stored_by_file = defaultdict(list)
    rows = conn.execute(
        "SELECT file_id,station_id,stop_id,route_id,prediction_time,arrival_time,is_approaching,is_delayed"
        + (",run_number " if include_runs else " ") +
        "FROM arrivals WHERE collection_timestamp BETWEEN %s AND %s AND file_id=ANY(%s)",
        (min(record[2] for record in files), max(record[2] for record in files),
         [record[0] for record in files]),
    ).fetchall()
    for file_id, *prediction in rows:
        stored_by_file[file_id].append(tuple(prediction))

    def read_source(record):
        try:
            if include_runs:
                return read_source_flags(client, bucket, record, routes, include_runs=True)
            return read_source_flags(client, bucket, record, routes)
        except ValueError as exc:
            if invalid_files is None:
                raise
            return exc

    with ThreadPoolExecutor(max_workers=workers) as executor:
        sources = list(executor.map(read_source, files))
    repairs = []
    for record, flags in zip(files, sources):
        try:
            if isinstance(flags, ValueError):
                raise flags
            planned = plan_boolean_repairs(record, stored_by_file[record[0]], flags, include_runs=include_runs)
        except ValueError as exc:
            if invalid_files is None:
                raise
            invalid_files.append({"file_id": record[0], "reason": str(exc)})
            continue
        repairs.extend(planned)
    return repairs


def database_fingerprint(conn):
    """Identify the actual PostgreSQL database, independent of DNS aliases or tunnels."""
    identity = conn.execute(
        "SELECT system_identifier::text,current_database(),oid::text "
        "FROM pg_control_system(),pg_database WHERE datname=current_database()"
    ).fetchone()
    if not identity or len(identity) != 3 or not all(isinstance(value, (str, int)) for value in identity):
        raise RuntimeError("Cannot verify PostgreSQL cluster/database identity; refusing checkpoint reuse.")
    return hashlib.sha256(json.dumps(list(map(str, identity)), separators=(",", ":")).encode()).hexdigest()


def save_checkpoint(path, after, cutoff, database, skipped=()):
    """Atomically advance only after a batch has committed successfully."""
    path = Path(path)
    path.parent.mkdir(parents=True, exist_ok=True)
    temporary = path.with_name(path.name + ".tmp")
    with temporary.open("w", encoding="utf-8") as output:
        json.dump({"version": 2, "kind": "booleans", "after": after,
                   "cutoff": cutoff, "database": database, "skipped": list(skipped)}, output)
        output.flush()
        os.fsync(output.fileno())
    os.replace(temporary, path)


def load_checkpoint(path, database, *, legacy_database=None):
    with Path(path).open(encoding="utf-8") as source:
        saved = json.load(source)
    version = saved.get("version")
    expected = database if version == 2 else legacy_database if version == 1 else None
    if expected is None or saved.get("kind") != "booleans" or saved.get("database") != expected:
        if version == 1:
            raise ValueError("Legacy checkpoint endpoint differs. Verify the production database before upgrading the checkpoint; do not delete progress.")
        raise ValueError("Checkpoint does not match this PostgreSQL database or repair kind.")
    after, cutoff = saved.get("after"), saved.get("cutoff")
    if not isinstance(after, int) or not isinstance(cutoff, int) or not 0 <= after <= cutoff:
        raise ValueError("Invalid checkpoint file-ID bounds.")
    return after, cutoff


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--kind", choices=["station-ids", "booleans"], required=True)
    parser.add_argument("--limit", type=int, default=25, help="Maximum files; default 25")
    parser.add_argument("--after-file-id", type=int, default=0, help="Resume after the reported file ID")
    parser.add_argument("--workers", type=int, default=16, help="Parallel XML reads, 1..32; default 16")
    parser.add_argument("--batch-size", type=int, default=100, help="Files per transaction, 1..1000; default 100")
    parser.add_argument("--all", action="store_true", help="Process all current boolean files in bounded batches")
    parser.add_argument("--checkpoint", help="Progress JSON path; --all defaults to logs/boolean_repair_checkpoint.json")
    parser.add_argument("--resume", action="store_true", help="Continue an automatic run from its saved checkpoint")
    parser.add_argument("--skip-invalid", action="store_true", help="Leave source-validation failures unchanged, record them, and continue")
    parser.add_argument("--public-url", action="store_true", help="Use DATABASE_PUBLIC_URL from Railway")
    parser.add_argument("--apply", action="store_true", help="Explicitly authorize database updates")
    args = parser.parse_args()
    maximum = 1000 if args.kind == "booleans" else 50000
    if not 1 <= args.limit <= maximum or args.after_file_id < 0:
        parser.error(f"Limit must be 1..{maximum}; after-file-id must be non-negative.")
    if not 1 <= args.workers <= 32 or not 1 <= args.batch_size <= 1000:
        parser.error("Workers must be 1..32; batch size must be 1..1000.")
    if args.kind != "booleans" and (args.all or args.resume or args.checkpoint or args.skip_invalid):
        parser.error("Automatic runs and checkpoints are for boolean repair only.")
    if args.resume and args.after_file_id:
        parser.error("Use either --resume or --after-file-id, not both.")
    if args.resume:
        args.all = True
    checkpoint = args.checkpoint or ("logs/boolean_repair_checkpoint.json" if args.all else None)
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
        legacy_database = hashlib.sha256(
            f"{conn.info.host}:{conn.info.port}/{conn.info.dbname}".encode()
        ).hexdigest()
        database = database_fingerprint(conn)
        cutoff = conn.execute("SELECT COALESCE(MAX(file_id),0) FROM ingestion_files").fetchone()[0]
        skipped = []
        if args.resume:
            args.after_file_id, cutoff = load_checkpoint(checkpoint, database, legacy_database=legacy_database)
            with Path(checkpoint).open(encoding="utf-8") as source:
                skipped = json.load(source).get("skipped", [])
        elif args.apply and checkpoint and Path(checkpoint).exists():
            parser.error("Checkpoint already exists; use --resume or choose a new --checkpoint path.")
        condition = "station_id IS NULL AND row_count=0" if args.kind == "station-ids" else "row_count>0"
        query = (
            "SELECT file_id,source_file,collection_timestamp,row_count FROM ingestion_files "
            "WHERE status='processed' AND file_id>%s AND file_id<=%s AND " + condition + " ORDER BY file_id LIMIT %s"
        )
        files = conn.execute(
            query, (args.after_file_id, cutoff, args.limit),
        ).fetchall()
        print(f"Mode: {'APPLY' if args.apply else 'DRY RUN (read-only)'} | Kind: {args.kind} | Files in first page: {len(files)} | Cutoff: {cutoff}", flush=True)
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
            client = get_bucket_client(max_pool_connections=args.workers)
            routes = {name: route_id for route_id, name in conn.execute("SELECT route_id,route_name FROM routes").fetchall()}
            run_aware = bool(conn.execute(
                "SELECT EXISTS (SELECT 1 FROM information_schema.columns WHERE table_schema='public' "
                "AND table_name='arrivals' AND column_name='run_number')"
            ).fetchone()[0])
            total = conn.execute(
                "SELECT COUNT(*) FROM ingestion_files WHERE status='processed' AND row_count>0 AND file_id>%s AND file_id<=%s",
                (args.after_file_id, cutoff),
            ).fetchone()[0]
            target = total if args.all else min(total, args.limit)
            changed = 0
            completed = 0
            started = monotonic()
            while files:
                for offset in range(0, len(files), args.batch_size):
                    batch = files[offset:offset + args.batch_size]
                    invalid = [] if args.skip_invalid else None
                    repairs = plan_boolean_batch(
                        conn, client, bucket, batch, routes, args.workers,
                        invalid_files=invalid,
                        include_runs=run_aware,
                    )
                    if args.apply:
                        apply_repairs(conn, RUN_FLAG_REPAIR_SQL if run_aware else FLAG_REPAIR_SQL, repairs)
                        if checkpoint:
                            save_checkpoint(checkpoint, batch[-1][0], cutoff, database, skipped + (invalid or []))
                    for item in invalid or []:
                        print(f"REVIEW file_id={item['file_id']}: {item['reason']} (unchanged)", flush=True)
                    skipped.extend(invalid or [])
                    changed += len(repairs)
                    completed += len(batch)
                    elapsed = monotonic() - started
                    rate = completed / max(elapsed, 0.001)
                    print(
                        f"Files: {completed}/{target} | Corrections: {changed} | "
                        f"Rate: {rate:.1f} files/s | Elapsed: {elapsed:.1f}s | "
                        f"ETA: {max(0, target-completed)/rate/60:.1f} min | "
                        f"Resume: --after-file-id {batch[-1][0]}", flush=True,
                    )
                if not args.all:
                    break
                args.after_file_id = files[-1][0]
                files = conn.execute(query, (args.after_file_id, cutoff, args.limit)).fetchall()
            print(f"Rows {'updated' if args.apply else 'that would change'}: {changed}")
            if args.all:
                print(f"Finished through fixed file-ID cutoff: {cutoff}", flush=True)
            if skipped:
                print(f"INCOMPLETE: {len(skipped)} source-validation failures remain unchanged and need review.", flush=True)
                raise SystemExit(2)
        if files:
            print(f"Next batch: --after-file-id {files[-1][0]}")


if __name__ == "__main__":
    main()