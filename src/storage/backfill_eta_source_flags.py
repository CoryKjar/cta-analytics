"""Backfill CTA isSch/isFlt values from archived XML, without replaying audits."""
import argparse
from collections import Counter
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
import hashlib
import os
from pathlib import Path

import psycopg

from src.storage.object_storage import get_bucket_client
from src.transform.arrival_quality import COLUMNS, POLICY, resolve_unknowns
from src.transform.parse_arrivals import source_prediction_records


MIGRATION = Path(__file__).resolve().parents[2] / "migrations" / "20261019_add_cta_eta_source_flags.sql"
LOCK_KEY = 2026101902


def _connect(read_only):
    database_url = (
        os.getenv("BACKFILL_DATABASE_URL")
        or os.getenv("DATABASE_PUBLIC_URL")
        or os.getenv("DATABASE_URL")
    )
    if not database_url:
        raise ValueError("No database URL is configured for the backfill.")
    options = "-c statement_timeout=120000 -c lock_timeout=5000"
    if read_only:
        options += " -c default_transaction_read_only=on"
    return psycopg.connect(database_url, connect_timeout=20, options=options)


def apply_migration():
    statements = [part.strip() for part in MIGRATION.read_text(encoding="utf-8").split(";")]
    with _connect(read_only=False) as connection:
        with connection.cursor() as cursor:
            for statement in statements:
                if statement:
                    cursor.execute(statement)
    print("migration_applied: ETA flags and backfill ledger")


def _require_migration(cursor):
    cursor.execute("""
        SELECT column_name FROM information_schema.columns
        WHERE table_schema='public' AND table_name='arrivals'
          AND column_name IN ('is_scheduled','is_fault')
    """)
    if {row[0] for row in cursor.fetchall()} != {"is_scheduled", "is_fault"}:
        raise RuntimeError("Apply the ETA source-flag migration first.")
    cursor.execute("SELECT to_regclass('public.arrival_source_flag_backfill')")
    if cursor.fetchone()[0] is None:
        raise RuntimeError("Backfill ledger is missing; apply the ETA source-flag migration.")


def _parse_bound(value):
    if value is None:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is not None:
        raise argparse.ArgumentTypeError("Use naive Chicago-local timestamps for bounds.")
    return parsed


def _manifest_batch(connection, last_key, start, end, limit, shard_count, shard_index):
    conditions = [
        "f.status='processed'", "f.row_count > 0",
        "NOT EXISTS (SELECT 1 FROM public.arrival_source_flag_backfill b WHERE b.file_id=f.file_id)",
    ]
    params = []
    if shard_count > 1:
        conditions.append("mod(f.file_id, %s) = %s")
        params.extend((shard_count, shard_index))
    if last_key is not None:
        conditions.append("(f.collection_timestamp,f.file_id) > (%s,%s)")
        params.extend(last_key)
    if start is not None:
        conditions.append("f.collection_timestamp >= %s")
        params.append(start)
    if end is not None:
        conditions.append("f.collection_timestamp < %s")
        params.append(end)
    query = (
        "SELECT f.file_id,f.source_file,f.collection_timestamp,f.row_count "
        "FROM public.ingestion_files AS f WHERE " + " AND ".join(conditions) +
        " ORDER BY f.collection_timestamp,f.file_id LIMIT %s"
    )
    params.append(limit)
    with connection.cursor() as cursor:
        cursor.execute(query, params)
        files = cursor.fetchall()
    connection.commit()
    return files


def _prepare_file(client, bucket, manifest_row, route_map, destination_map):
    file_id, source_file, collected, manifest_rows = manifest_row
    key = collected.strftime("raw/%Y/%m/%d/") + source_file
    response = client.get_object(Bucket=bucket, Key=key)
    try:
        source_xml = response["Body"].read()
    finally:
        response["Body"].close()
    source_rows = source_prediction_records(
        source_xml, (file_id, source_file, collected, manifest_rows),
        route_map, destination_map,
    )
    canonical_rows, decisions = resolve_unknowns(source_rows)
    if len(canonical_rows) != manifest_rows:
        raise ValueError("Canonical row count differs from manifest.")
    return {
        "file_id": file_id,
        "source_hash": hashlib.sha256(source_xml).hexdigest(),
        "source_count": len(source_rows),
        "rows": canonical_rows,
        "decision_count": len(decisions),
    }


def _source_key(row):
    return row[5], row[0], row[2], row[3], row[8], row[17]


def _plan_updates(connection, prepared, apply):
    file_ids = [item["file_id"] for item in prepared]
    if not file_ids:
        return [], [], 0, []
    statement = """
        SELECT file_id,collection_timestamp,station_id,stop_id,route_id,
               arrival_time,run_number,is_scheduled,is_fault
        FROM public.arrivals WHERE file_id=ANY(%s)
    """
    if apply:
        statement += " FOR UPDATE"
    with connection.cursor() as cursor:
        cursor.execute(statement, (file_ids,))
        targets = cursor.fetchall()
    if not apply:
        connection.commit()

    target_by_key = {}
    for target in targets:
        key = target[0], target[1], target[3], target[4], target[5], target[6]
        if key in target_by_key:
            raise ValueError("Duplicate database arrival identity.")
        target_by_key[key] = target

    staged, ledger, errors = [], [], []
    expected_updates = 0
    for item in prepared:
        try:
            source_by_key = {_source_key(row): row for row in item["rows"]}
            if len(source_by_key) != len(item["rows"]):
                raise ValueError("Duplicate canonical source identity.")
            target_keys = {key for key in target_by_key if key[0] == item["file_id"]}
            if target_keys != set(source_by_key):
                raise ValueError("Source identities do not exactly match database rows.")
            file_stage = []
            changed = unknown_scheduled = unknown_fault = 0
            for key, row in source_by_key.items():
                target = target_by_key[key]
                if target[2] != row[1]:
                    raise ValueError("Source station differs from database row.")
                scheduled = row[COLUMNS.index("is_scheduled")]
                fault = row[COLUMNS.index("is_fault")]
                unknown_scheduled += scheduled is None
                unknown_fault += fault is None
                changed += target[7] != scheduled or target[8] != fault
                file_stage.append((*key, scheduled, fault))
            staged.extend(file_stage)
            expected_updates += changed
            ledger.append((
                item["file_id"], POLICY, item["source_hash"], item["source_count"],
                len(source_by_key), len(source_by_key), changed,
                unknown_scheduled, unknown_fault, item["decision_count"],
            ))
        except Exception as exc:
            errors.append((item["file_id"], type(exc).__name__))
    return staged, ledger, expected_updates, errors


def _create_stage(connection):
    with connection.transaction():
        connection.execute("""
            CREATE TEMP TABLE cta_eta_flag_stage (
                file_id INTEGER NOT NULL,
                collection_timestamp TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                stop_id INTEGER NOT NULL,
                route_id SMALLINT NOT NULL,
                arrival_time TIMESTAMP WITHOUT TIME ZONE NOT NULL,
                run_number TEXT NOT NULL,
                is_scheduled BOOLEAN,
                is_fault BOOLEAN,
                PRIMARY KEY(file_id,collection_timestamp,stop_id,route_id,arrival_time,run_number)
            ) ON COMMIT DELETE ROWS
        """)


def _apply_batch(connection, staged, ledger, expected_updates):
    with connection.transaction():
        with connection.cursor() as cursor:
            cursor.execute("TRUNCATE pg_temp.cta_eta_flag_stage")
            with cursor.copy("""
                COPY pg_temp.cta_eta_flag_stage (
                    file_id,collection_timestamp,stop_id,route_id,arrival_time,
                    run_number,is_scheduled,is_fault
                ) FROM STDIN
            """) as copy:
                for row in staged:
                    copy.write_row(row)
            cursor.execute("""
                UPDATE public.arrivals AS a
                SET is_scheduled=s.is_scheduled, is_fault=s.is_fault
                FROM pg_temp.cta_eta_flag_stage AS s
                WHERE a.file_id=s.file_id
                  AND a.collection_timestamp=s.collection_timestamp
                  AND a.stop_id=s.stop_id AND a.route_id=s.route_id
                  AND a.arrival_time=s.arrival_time AND a.run_number=s.run_number
                  AND (a.is_scheduled IS DISTINCT FROM s.is_scheduled
                       OR a.is_fault IS DISTINCT FROM s.is_fault)
            """)
            updated = cursor.rowcount
            if updated != expected_updates:
                raise RuntimeError("Updated rows differ from the verified plan; batch rolled back.")
            cursor.executemany("""
                INSERT INTO public.arrival_source_flag_backfill (
                    file_id,policy,source_sha256,source_row_count,canonical_row_count,
                    matched_row_count,updated_row_count,unknown_scheduled_rows,
                    unknown_fault_rows,resolution_groups
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)
            """, ledger)
            return updated


def _load_dimensions(connection):
    with connection.cursor() as cursor:
        cursor.execute("SELECT route_id,route_name FROM public.routes")
        routes = {name: route_id for route_id, name in cursor.fetchall()}
        cursor.execute("SELECT destination_id,destination_name FROM public.destinations")
        destinations = {name: destination_id for destination_id, name in cursor.fetchall()}
    connection.commit()
    return routes, destinations


def run_backfill(args):
    connection = _connect(read_only=not args.apply)
    with connection.cursor() as cursor:
        _require_migration(cursor)
    connection.commit()
    route_map, destination_map = _load_dimensions(connection)
    client = get_bucket_client(max_pool_connections=args.workers)
    bucket = os.environ.get("BUCKET")
    if not bucket:
        raise ValueError("BUCKET is not configured.")

    if args.apply:
        with connection.cursor() as cursor:
            cursor.execute("SELECT pg_try_advisory_lock(%s, %s)", (LOCK_KEY, args.shard_index))
            if not cursor.fetchone()[0]:
                raise RuntimeError("Another source-flag backfill is already running.")
        connection.commit()
        _create_stage(connection)

    totals = Counter()
    last_key = None
    max_files = args.max_files if args.max_files is not None else (None if args.all else 100)
    next_progress = args.progress_every
    executor = ThreadPoolExecutor(max_workers=args.workers)
    try:
        while max_files is None or totals["files_scanned"] < max_files:
            limit = args.batch_files
            if max_files is not None:
                limit = min(limit, max_files - totals["files_scanned"])
            files = _manifest_batch(
                connection, last_key, args.start, args.end, limit,
                args.shard_count, args.shard_index,
            )
            if not files:
                break
            last_key = files[-1][2], files[-1][0]
            totals["files_scanned"] += len(files)
            futures = {
                executor.submit(_prepare_file, client, bucket, row, route_map, destination_map): row
                for row in files
            }
            prepared = []
            for future in as_completed(futures):
                row = futures[future]
                try:
                    prepared.append(future.result())
                except Exception as exc:
                    totals["file_errors"] += 1
                    totals["error_type_" + type(exc).__name__] += 1
                    if totals["file_errors"] <= 10:
                        print(f"file_skipped: id={row[0]} error={type(exc).__name__}")

            staged, ledger, updates, errors = _plan_updates(connection, prepared, args.apply)
            for file_id, error_type in errors:
                totals["file_errors"] += 1
                totals["error_type_" + error_type] += 1
                if totals["file_errors"] <= 10:
                    print(f"file_skipped: id={file_id} error={error_type}")
            if ledger:
                totals["files_validated"] += len(ledger)
                totals["source_rows"] += sum(row[3] for row in ledger)
                totals["matched_rows"] += sum(row[5] for row in ledger)
                totals["rows_needing_update"] += updates
                totals["unknown_scheduled_rows"] += sum(row[7] for row in ledger)
                totals["unknown_fault_rows"] += sum(row[8] for row in ledger)
                if args.apply:
                    totals["rows_updated"] += _apply_batch(connection, staged, ledger, updates)
                    connection.commit()
                    totals["files_recorded"] += len(ledger)
            if totals["files_scanned"] >= next_progress:
                print(
                    f"progress: scanned={totals['files_scanned']} "
                    f"validated={totals['files_validated']} updated={totals['rows_updated']} "
                    f"errors={totals['file_errors']}"
                )
                next_progress += args.progress_every
    finally:
        executor.shutdown(wait=True)
        if args.apply:
            try:
                with connection.cursor() as cursor:
                    cursor.execute("SELECT pg_advisory_unlock(%s, %s)", (LOCK_KEY, args.shard_index))
                connection.commit()
            except Exception:
                connection.rollback()
        connection.close()

    print("backfill_summary:", dict(totals))
    if totals["file_errors"]:
        raise SystemExit(2)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--apply-migration", action="store_true")
    parser.add_argument("--apply", action="store_true", help="write verified flag values")
    scope = parser.add_mutually_exclusive_group()
    scope.add_argument("--all", action="store_true", help="process all eligible files")
    scope.add_argument("--max-files", type=int, help="maximum files; defaults to 100 for dry runs")
    parser.add_argument("--start", type=_parse_bound, help="inclusive Chicago-local timestamp")
    parser.add_argument("--end", type=_parse_bound, help="exclusive Chicago-local timestamp")
    parser.add_argument("--batch-files", type=int, default=100)
    parser.add_argument("--workers", type=int, default=64)
    parser.add_argument("--shard-count", type=int, default=1)
    parser.add_argument("--shard-index", type=int, default=0)
    parser.add_argument("--progress-every", type=int, default=1000)
    args = parser.parse_args()
    if args.apply_migration:
        if args.apply or args.all or args.max_files is not None or args.start or args.end:
            parser.error("--apply-migration cannot be combined with backfill options")
        apply_migration()
        return
    if args.apply and not (args.all or args.max_files is not None):
        parser.error("--apply requires an explicit --all or --max-files bound")
    if args.max_files is not None and args.max_files < 1:
        parser.error("--max-files must be positive")
    if args.batch_files < 1 or args.workers < 1 or args.progress_every < 1:
        parser.error("batch size, workers, and progress interval must be positive")
    if args.shard_count < 1 or not 0 <= args.shard_index < args.shard_count:
        parser.error("shard index must be in [0, shard-count)")
    if args.start and args.end and args.start >= args.end:
        parser.error("--start must be earlier than --end")
    run_backfill(args)


if __name__ == "__main__":
    main()