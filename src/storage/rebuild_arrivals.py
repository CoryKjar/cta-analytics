"""Resumable source-backed arrivals rebuild in an isolated schema; no implicit cutover."""
import argparse
import hashlib
import json
import os
import re
from concurrent.futures import ThreadPoolExecutor
from datetime import timedelta
from time import monotonic
from zoneinfo import ZoneInfo

import psycopg
from psycopg import sql
from dotenv import load_dotenv

from src.config.stations import station_id_from_filename
from src.storage.object_storage import get_bucket_client
from src.storage.repair_data_quality import database_fingerprint
from src.storage.restore_train_identity import COLUMNS, source_records, source_prediction_records


def rebuild_schema(value):
    if not re.fullmatch(r"cta_rebuild_[a-z0-9_]{1,40}", value):
        raise ValueError("Use an isolated schema name starting cta_rebuild_ (lowercase letters/digits/underscores).")
    return value


def table(schema, name):
    return sql.Identifier(rebuild_schema(schema), name)


def copy_rows(cursor, destination, columns, rows):
    statement = sql.SQL("COPY {} ({}) FROM STDIN").format(
        destination, sql.SQL(",").join(map(sql.Identifier, columns)),
    )
    with cursor.copy(statement) as stream:
        for row in rows:
            stream.write_row(row)


def acquire_worker_lock(conn, schema):
    locked = conn.execute("SELECT pg_try_advisory_lock(hashtextextended(%s,0))", (rebuild_schema(schema),)).fetchone()[0]
    if not locked:
        raise RuntimeError("Another operation is already using this rebuild schema.")


def verify_database(conn, schema, *, allow_complete=False):
    row = conn.execute(sql.SQL("SELECT database_identity,completed_at FROM {} WHERE singleton").format(table(schema, "state"))).fetchone()
    if not row or row[0] != database_fingerprint(conn):
        raise RuntimeError("Rebuild state does not belong to this database.")
    if row[1] is not None and not allow_complete:
        raise RuntimeError("This rebuild has already cut over; its staging worker must not restart.")


def initialize(conn, schema):
    rebuild_schema(schema)
    fingerprint = database_fingerprint(conn)
    with conn.transaction():
        conn.execute(sql.SQL("CREATE SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("""
            CREATE TABLE {} (
                singleton boolean PRIMARY KEY DEFAULT true CHECK(singleton),
                database_identity text NOT NULL, cutoff bigint NOT NULL,
                created_at timestamptz NOT NULL DEFAULT NOW(), inventory_at timestamptz,
                validated_at timestamptz, completed_at timestamptz
            )
        """).format(table(schema, "state")))
        conn.execute(sql.SQL("INSERT INTO {} (database_identity,cutoff) SELECT %s,COALESCE(MAX(file_id),0) FROM public.ingestion_files").format(table(schema, "state")), (fingerprint,))
        conn.execute(sql.SQL("""
            CREATE TABLE {} (
                file_id integer PRIMARY KEY, source_file text UNIQUE NOT NULL,
                collection_timestamp timestamp NOT NULL, snapshot_status text NOT NULL,
                snapshot_row_count integer NOT NULL, object_key text, etag text,
                status text NOT NULL DEFAULT 'pending' CHECK(status IN ('pending','loaded','error')),
                row_count integer, source_sha256 text, station_id integer,
                error text, loaded_at timestamptz
            )
        """).format(table(schema, "files")))
        conn.execute(sql.SQL("""
            INSERT INTO {} (file_id,source_file,collection_timestamp,snapshot_status,snapshot_row_count)
            SELECT file_id,source_file,collection_timestamp,status,row_count FROM public.ingestion_files
            WHERE file_id <= (SELECT cutoff FROM {} WHERE singleton)
        """).format(table(schema, "files"), table(schema, "state")))
        conn.execute(sql.SQL("""
            CREATE TABLE {} (
                object_key text PRIMARY KEY, source_file text NOT NULL, etag text NOT NULL,
                modified_at timestamptz NOT NULL
            )
        """).format(table(schema, "inventory")))
        conn.execute(sql.SQL("CREATE TABLE {} (LIKE public.arrivals INCLUDING DEFAULTS INCLUDING CONSTRAINTS)").format(table(schema, "arrivals")))
        conn.execute(sql.SQL("CREATE UNIQUE INDEX arrivals_source_identity ON {} (collection_timestamp,stop_id,route_id,arrival_time,run_number) NULLS NOT DISTINCT").format(table(schema, "arrivals")))
        conn.execute(sql.SQL("CREATE INDEX arrivals_file_id ON {} (file_id)").format(table(schema, "arrivals")))
        conn.execute(sql.SQL("ALTER TABLE {} ADD FOREIGN KEY(file_id) REFERENCES public.ingestion_files(file_id)").format(table(schema, "arrivals")))
        for column, dimension in (("station_id", "stations"), ("route_id", "routes"), ("destination_id", "destinations")):
            conn.execute(sql.SQL("ALTER TABLE {} ADD FOREIGN KEY({}) REFERENCES public.{}({})").format(
                table(schema, "arrivals"), sql.Identifier(column), sql.Identifier(dimension), sql.Identifier(column),
            ))
    print("Initialized isolated rebuild schema:", schema, flush=True)


def inventory(conn, client, bucket, schema, *, recent=False):
    verify_database(conn, schema)
    created, previous = conn.execute(sql.SQL("SELECT created_at,inventory_at FROM {} WHERE singleton").format(table(schema, "state"))).fetchone()
    prefixes = ["raw/"]
    if recent:
        if previous is None:
            raise ValueError("A full inventory must precede recent-day refresh.")
        start = previous.astimezone(ZoneInfo("America/Chicago")).date()
        end = created.astimezone(ZoneInfo("America/Chicago")).date()
        days = {start + timedelta(days=index) for index in range((end-start).days + 1)}
        days.update(row[0] for row in conn.execute(sql.SQL("SELECT DISTINCT collection_timestamp::date FROM {} WHERE status='pending'").format(table(schema, "files"))).fetchall())
        prefixes = [day.strftime("raw/%Y/%m/%d/") for day in sorted(days)]
    count = 0
    pages = (page for prefix in prefixes for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=prefix))
    for page in pages:
        rows = [(item["Key"], item["Key"].rsplit("/", 1)[-1], item["ETag"], item["LastModified"])
                for item in page.get("Contents", [])
                if item["Key"].lower().endswith(".xml") and item["LastModified"] <= created]
        with conn.transaction():
            with conn.cursor() as cursor:
                cursor.execute("CREATE TEMP TABLE inventory_batch (object_key text,source_file text,etag text,modified_at timestamptz) ON COMMIT DROP")
                copy_rows(cursor, sql.Identifier("inventory_batch"), ("object_key", "source_file", "etag", "modified_at"), rows)
                cursor.execute(sql.SQL("INSERT INTO {} SELECT * FROM inventory_batch ON CONFLICT(object_key) DO UPDATE SET etag=EXCLUDED.etag,modified_at=EXCLUDED.modified_at,source_file=EXCLUDED.source_file").format(table(schema, "inventory")))
        count += len(rows)
        if count and count % 25000 < 1000:
            print("Bucket objects inventoried:", count, flush=True)
    duplicates = conn.execute(sql.SQL("SELECT source_file,COUNT(*) FROM {} GROUP BY source_file HAVING COUNT(*)>1 LIMIT 5").format(table(schema, "inventory"))).fetchall()
    if duplicates:
        raise ValueError(f"Duplicate bucket basenames prevent a safe rebuild: {duplicates}")
    with conn.transaction():
        conn.execute(sql.SQL("UPDATE {} f SET object_key=i.object_key,etag=i.etag FROM {} i WHERE f.source_file=i.source_file AND f.status<>'loaded'").format(table(schema, "files"), table(schema, "inventory")))
        conn.execute(sql.SQL("UPDATE {} SET inventory_at=NOW(),validated_at=NULL WHERE singleton").format(table(schema, "state")))
    print("Inventory complete:", count, "objects at/before snapshot start", flush=True)


def read_prediction_file(client, bucket, record, routes, destinations, *, with_audit=False):
    file_id, filename, collected, old_count, key, etag = record
    if not key or not etag:
        raise ValueError("Snapshot file is missing from the verified bucket inventory.")
    response = client.get_object(Bucket=bucket, Key=key, IfMatch=etag)
    try:
        data = response["Body"].read()
    finally:
        response["Body"].close()
    decisions = []
    if with_audit:
        from src.storage.resolve_rebuild_conflicts import resolve_unknowns
        rows, decisions = resolve_unknowns(source_prediction_records(data, (file_id, filename, collected, old_count), routes, destinations))
    else:
        rows = source_records(data, (file_id, filename, collected, old_count), routes, destinations, allow_empty=True)
    station_id = rows[0][1] if rows else station_id_from_filename(filename)
    if station_id is None:
        raise ValueError("Empty source filename has no known station identity.")
    configured_station = station_id_from_filename(filename)
    if configured_station is not None and station_id != configured_station:
        raise ValueError("API station identity does not match the configured filename.")
    result = (rows, station_id, hashlib.sha256(data).hexdigest())
    return (*result, (data, decisions) if decisions else None) if with_audit else result


def build(conn, client, bucket, schema, *, batch_size, workers, limit, retry_errors=False):
    from src.storage.quality_audit import ensure_audit_table, write_quality_audit
    verify_database(conn, schema)
    ensure_audit_table(conn, table(schema, "resolution_audit"), table(schema, "files"))
    if not conn.execute(sql.SQL("SELECT inventory_at IS NOT NULL AND inventory_at>=created_at FROM {} WHERE singleton").format(table(schema, "state"))).fetchone()[0]:
        raise ValueError("Complete bucket inventory before building.")
    if retry_errors:
        conn.execute(sql.SQL("UPDATE {} SET status='pending',error=NULL WHERE status='error'").format(table(schema, "files")))
    routes = {name: identity for identity, name in conn.execute("SELECT route_id,route_name FROM public.routes").fetchall()}
    destinations = {name: identity for identity, name in conn.execute("SELECT destination_id,destination_name FROM public.destinations").fetchall()}
    stations = {row[0] for row in conn.execute("SELECT station_id FROM public.stations").fetchall()}
    processed = changed = failures = 0
    started = monotonic()

    def read(record):
        try:
            rows, station, digest, evidence = read_prediction_file(client, bucket, record, routes, destinations, with_audit=True)
            if station not in stations:
                raise ValueError("Source station is absent from the database dimension.")
            return record, rows, station, digest, None, evidence
        except Exception as exc:
            reason = str(exc)[:500] if isinstance(exc, ValueError) else type(exc).__name__
            return record, [], None, None, reason, None

    with ThreadPoolExecutor(max_workers=workers) as executor:
        while not limit or processed < limit:
            size = min(batch_size, limit - processed) if limit else batch_size
            files = conn.execute(sql.SQL("""
                SELECT file_id,source_file,collection_timestamp,snapshot_row_count,object_key,etag
                FROM {} WHERE status='pending' ORDER BY file_id LIMIT %s
            """).format(table(schema, "files")), (size,)).fetchall()
            if not files:
                break
            results = list(executor.map(read, files))
            with conn.transaction():
                with conn.cursor() as cursor:
                    copy_rows(cursor, table(schema, "arrivals"), COLUMNS,
                              (row for result in results if not result[4] for row in result[1]))
                    successful, errors = [], []
                    for record, rows, station, digest, error, evidence in results:
                        if error:
                            errors.append((error, record[0]))
                            failures += 1
                        else:
                            if evidence:
                                data, decisions = evidence
                                write_quality_audit(cursor, table(schema, "resolution_audit"), record[:5], rows, data, decisions, etag=record[5])
                            successful.append((len(rows), station, digest, record[0]))
                            changed += len(rows)
                    if successful:
                        cursor.executemany(sql.SQL("UPDATE {} SET status='loaded',row_count=%s,station_id=%s,source_sha256=%s,error=NULL,loaded_at=NOW() WHERE file_id=%s").format(table(schema, "files")), successful)
                    if errors:
                        cursor.executemany(sql.SQL("UPDATE {} SET status='error',error=%s WHERE file_id=%s").format(table(schema, "files")), errors)
                    cursor.execute(sql.SQL("UPDATE {} SET validated_at=NULL WHERE singleton").format(table(schema, "state")))
            processed += len(files)
            print(f"Files committed: {processed} | Rows staged: {changed} | Errors: {failures} | Rate: {processed/max(0.001,monotonic()-started):.1f} files/s", flush=True)
    print("Build pass finished. Only staging data was changed.", flush=True)


def catch_up(conn, schema):
    verify_database(conn, schema)
    with conn.transaction():
        conn.execute(sql.SQL("UPDATE {} SET cutoff=(SELECT COALESCE(MAX(file_id),0) FROM public.ingestion_files),created_at=NOW(),validated_at=NULL WHERE singleton").format(table(schema, "state")))
        conn.execute(sql.SQL("""
            INSERT INTO {} (file_id,source_file,collection_timestamp,snapshot_status,snapshot_row_count)
            SELECT file_id,source_file,collection_timestamp,status,row_count FROM public.ingestion_files
            WHERE file_id <= (SELECT cutoff FROM {} WHERE singleton)
            ON CONFLICT(file_id) DO NOTHING
        """).format(table(schema, "files"), table(schema, "state")))
    print("Snapshot advanced. Rerun inventory, build pending files, then validate.", flush=True)


def validation_report(conn, schema):
    from src.storage.resolve_rebuild_conflicts import audit_validation_report
    verify_database(conn, schema)
    report = {}
    report["file_states"] = dict(conn.execute(sql.SQL("SELECT status,COUNT(*) FROM {} GROUP BY status").format(table(schema, "files"))).fetchall())
    report["missing_source_objects"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE object_key IS NULL OR etag IS NULL").format(table(schema, "files"))).fetchone()[0]
    report["duplicate_basenames"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM (SELECT source_file FROM {} GROUP BY source_file HAVING COUNT(*)>1) duplicates").format(table(schema, "inventory"))).fetchone()[0]
    report["unmanifested_old_objects"] = conn.execute(sql.SQL("""
        SELECT COUNT(*) FROM {} i LEFT JOIN public.ingestion_files f ON f.source_file=i.source_file
        WHERE f.file_id IS NULL
    """).format(table(schema, "inventory"))).fetchone()[0]
    report["file_row_count_mismatches"] = conn.execute(sql.SQL("""
        SELECT COUNT(*) FROM (SELECT f.file_id FROM {} f LEFT JOIN {} a ON a.file_id=f.file_id
        WHERE f.status='loaded' GROUP BY f.file_id,f.row_count HAVING COUNT(a.file_id)<>f.row_count) mismatches
    """).format(table(schema, "files"), table(schema, "arrivals"))).fetchone()[0]
    totals = conn.execute(sql.SQL("""
        SELECT COUNT(*),COUNT(*) FILTER(WHERE run_number IS NULL),
        COUNT(*) FILTER(WHERE wait_time IS NULL OR ABS(wait_time-EXTRACT(EPOCH FROM (arrival_time-prediction_time))/60)>0.02),
        COUNT(*) FILTER(WHERE latitude NOT BETWEEN -90 AND 90 OR longitude NOT BETWEEN -180 AND 180)
        FROM {}
    """).format(table(schema, "arrivals"))).fetchone()
    report.update(zip(("staged_rows", "missing_run_numbers", "invalid_wait_times", "out_of_range_coordinates"), totals))
    report["source_rows"] = conn.execute(sql.SQL("SELECT COALESCE(SUM(row_count),0)::bigint FROM {} WHERE status='loaded'").format(table(schema, "files"))).fetchone()[0]
    report["pending_live_delta"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM public.ingestion_files WHERE file_id>(SELECT cutoff FROM {} WHERE singleton)").format(table(schema, "state"))).fetchone()[0]
    report["changed_source_objects"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM {} f JOIN {} i ON f.source_file=i.source_file WHERE f.status='loaded' AND f.etag IS DISTINCT FROM i.etag").format(table(schema, "files"), table(schema, "inventory"))).fetchone()[0]
    report["unprocessed_live_files"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM public.ingestion_files f JOIN {} s USING(file_id) WHERE f.status<>'processed'").format(table(schema, "files"))).fetchone()[0]
    report["missing_source_hashes"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE status='loaded' AND (source_sha256 IS NULL OR source_sha256 !~ %s)").format(table(schema, "files")), ("^[0-9a-f]{64}$",)).fetchone()[0]
    report["station_metadata_mismatches"] = conn.execute(sql.SQL("SELECT COUNT(*) FROM public.ingestion_files f JOIN {} s USING(file_id) WHERE s.status='loaded' AND f.station_id IS NOT NULL AND f.station_id<>s.station_id").format(table(schema, "files"))).fetchone()[0]
    report.update(audit_validation_report(conn, schema))
    report["ready_for_snapshot"] = (
        bool(sum(report["file_states"].values())) and not report["file_states"].get("pending", 0)
        and not report["file_states"].get("error", 0)
        and all(not report[key] for key in ("missing_source_objects", "duplicate_basenames", "unmanifested_old_objects", "file_row_count_mismatches", "missing_run_numbers", "invalid_wait_times", "out_of_range_coordinates", "changed_source_objects", "missing_source_hashes", "station_metadata_mismatches", "resolution_audit_failures"))
        and report["source_rows"] == report["staged_rows"]
    )
    return report


def cutover(conn, schema, *, writers_paused):
    if not writers_paused:
        raise ValueError("Stop collector, recovery, repair jobs and writable SQL clients first; then explicitly acknowledge --writers-paused.")
    verify_database(conn, schema)
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='3s'")
        conn.execute(sql.SQL("LOCK TABLE public.ingestion_files,public.arrivals,{},{} IN ACCESS EXCLUSIVE MODE").format(table(schema, "arrivals"), table(schema, "files")))
        report = validation_report(conn, schema)
        if not report["ready_for_snapshot"] or report["pending_live_delta"] or report["unprocessed_live_files"]:
            raise ValueError("Cutover blocked: validation, live delta or nonprocessed files remain.")
        dependents = conn.execute("""
            SELECT conrelid::regclass::text FROM pg_constraint
            WHERE contype='f' AND confrelid='public.arrivals'::regclass
        """).fetchall()
        views = conn.execute("""
            SELECT DISTINCT c.relname FROM pg_depend d JOIN pg_rewrite r ON r.oid=d.objid
            JOIN pg_class c ON c.oid=r.ev_class WHERE d.refobjid='public.arrivals'::regclass
        """).fetchall()
        security = conn.execute("SELECT relrowsecurity,relforcerowsecurity FROM pg_class WHERE oid='public.arrivals'::regclass").fetchone()
        triggers = conn.execute("SELECT COUNT(*) FROM pg_trigger WHERE tgrelid='public.arrivals'::regclass AND NOT tgisinternal").fetchone()[0]
        if dependents or views or triggers or any(security):
            raise ValueError("Dependent views/FKs/triggers/RLS require a separately reviewed cutover; live table unchanged.")
        acl = conn.execute("SELECT relacl FROM pg_class WHERE oid='public.arrivals'::regclass").fetchone()[0]
        owner = conn.execute("SELECT pg_get_userbyid(relowner) FROM pg_class WHERE oid='public.arrivals'::regclass").fetchone()[0]
        if acl is not None or owner != conn.execute("SELECT current_user").fetchone()[0]:
            raise ValueError("Custom grants/ownership require explicit preservation before cutover.")
        backup_name = schema + "_previous"
        if conn.execute("SELECT to_regclass(%s)", ("public." + backup_name,)).fetchone()[0]:
            raise ValueError("The rollback table already exists; refusing to overwrite it.")
        live_key = conn.execute("SELECT COUNT(*) FROM pg_constraint WHERE conrelid='public.arrivals'::regclass AND conname='arrivals_run_identity_unique' AND contype='u'").fetchone()[0]
        if live_key != 1:
            raise ValueError("The canonical production train-identity constraint is missing; review cutover explicitly.")
        conn.execute(sql.SQL("INSERT INTO public.arrival_resolution_audit SELECT * FROM {} ON CONFLICT(file_id) DO NOTHING").format(table(schema, "resolution_audit")))
        audit_mismatches = conn.execute(sql.SQL("""
            SELECT COUNT(*) FROM {} s LEFT JOIN public.arrival_resolution_audit p USING(file_id)
            WHERE p.file_id IS NULL OR
            (p.policy,p.source_sha256,p.evidence,p.canonical_row_count) IS DISTINCT FROM
            (s.policy,s.source_sha256,s.evidence,s.canonical_row_count)
        """).format(table(schema, "resolution_audit"))).fetchone()[0]
        if audit_mismatches:
            raise ValueError("Live audit evidence differs from staged resolution; cutover rolled back.")
        conn.execute(sql.SQL("CREATE TABLE {} AS SELECT f.* FROM public.ingestion_files f JOIN {} s USING(file_id)").format(table(schema, "manifest_before"), table(schema, "files")))
        conn.execute(sql.SQL("UPDATE public.ingestion_files f SET row_count=s.row_count,station_id=COALESCE(f.station_id,s.station_id) FROM {} s WHERE f.file_id=s.file_id").format(table(schema, "files")))
        conn.execute(sql.SQL("ALTER TABLE public.arrivals RENAME CONSTRAINT arrivals_run_identity_unique TO {}").format(sql.Identifier(schema + "_old_key")))
        conn.execute(sql.SQL("ALTER TABLE {} ADD CONSTRAINT arrivals_run_identity_unique UNIQUE USING INDEX arrivals_source_identity").format(table(schema, "arrivals")))
        conn.execute(sql.SQL("ALTER TABLE public.arrivals RENAME TO {}").format(sql.Identifier(backup_name)))
        conn.execute(sql.SQL("ALTER TABLE {} SET SCHEMA public").format(table(schema, "arrivals")))
        conn.execute(sql.SQL("UPDATE {} SET completed_at=NOW(),validated_at=NOW() WHERE singleton").format(table(schema, "state")))
    print("Cutover committed. Previous arrivals retained as public." + backup_name, flush=True)
    print("Do not drop the rollback table. Validate consumers and resume compatible writers.", flush=True)


def rollback(conn, schema, *, writers_paused):
    if not writers_paused:
        raise ValueError("Pause all writers and explicitly acknowledge --writers-paused before rollback.")
    verify_database(conn, schema, allow_complete=True)
    backup_name = schema + "_previous"
    with conn.transaction():
        conn.execute("SET LOCAL lock_timeout='3s'")
        state = conn.execute(sql.SQL("SELECT cutoff,completed_at FROM {} WHERE singleton FOR UPDATE").format(table(schema, "state"))).fetchone()
        if not state or state[1] is None or not conn.execute("SELECT to_regclass(%s)", ("public." + backup_name,)).fetchone()[0]:
            raise ValueError("No retained completed cutover is available to roll back.")
        conn.execute(sql.SQL("LOCK TABLE public.ingestion_files,public.arrivals,public.{} IN ACCESS EXCLUSIVE MODE").format(sql.Identifier(backup_name)))
        new_files = conn.execute("SELECT COUNT(*) FROM public.ingestion_files WHERE file_id>%s", (state[0],)).fetchone()[0]
        changed = conn.execute(sql.SQL("""
            SELECT COUNT(*) FROM public.ingestion_files f JOIN {} b USING(file_id)
            WHERE (f.source_file,f.collection_timestamp,f.status,f.attempts,f.last_error,
                   f.created_at,f.updated_at,f.processed_at)
            IS DISTINCT FROM
                  (b.source_file,b.collection_timestamp,b.status,b.attempts,b.last_error,
                   b.created_at,b.updated_at,b.processed_at)
        """).format(table(schema, "manifest_before"))).fetchone()[0]
        if new_files or changed:
            raise ValueError("Live ingestion/manifest changes occurred after cutover; reconcile them before rollback.")
        conn.execute(sql.SQL("ALTER TABLE public.arrivals SET SCHEMA {}").format(sql.Identifier(schema)))
        conn.execute(sql.SQL("ALTER TABLE public.{} RENAME TO arrivals").format(sql.Identifier(backup_name)))
        conn.execute(sql.SQL("ALTER TABLE public.arrivals RENAME CONSTRAINT {} TO arrivals_run_identity_unique").format(sql.Identifier(schema + "_old_key")))
        conn.execute(sql.SQL("UPDATE public.ingestion_files f SET row_count=b.row_count,station_id=b.station_id FROM {} b WHERE f.file_id=b.file_id").format(table(schema, "manifest_before")))
        conn.execute(sql.SQL("ALTER TABLE {} ADD COLUMN IF NOT EXISTS rolled_back_at timestamptz").format(table(schema, "state")))
        conn.execute(sql.SQL("UPDATE {} SET rolled_back_at=NOW() WHERE singleton").format(table(schema, "state")))
    print("Rollback committed. Prior live table restored; rebuilt table retained in staging. No tables deleted.", flush=True)


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("phase", choices=["init", "inventory", "build", "validate", "status", "catch-up", "cutover", "rollback"])
    parser.add_argument("--schema", required=True)
    parser.add_argument("--apply", action="store_true", help="Authorize the selected phase; only explicit cutover replaces live arrivals")
    parser.add_argument("--public-url", action="store_true")
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=250)
    parser.add_argument("--limit", type=int, default=0, help="Maximum files this pass; 0 means all pending files")
    parser.add_argument("--retry-errors", action="store_true")
    parser.add_argument("--recent", action="store_true", help="Refresh current/pending source days after a prior full inventory")
    parser.add_argument("--writers-paused", action="store_true", help="Explicit maintenance-window acknowledgement for cutover")
    args = parser.parse_args()
    rebuild_schema(args.schema)
    if not 1 <= args.workers <= 32 or not 1 <= args.batch_size <= 1000 or args.limit < 0:
        parser.error("Workers must be 1..32, batch size 1..1000, limit nonnegative.")
    writes = args.phase in ("init", "inventory", "build", "catch-up", "cutover", "rollback")
    if writes and not args.apply:
        parser.error("This phase requires --apply; cutover also requires --writers-paused.")
    load_dotenv()
    url = os.getenv("DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL")
    if not url:
        parser.error("The selected database URL is not configured.")
    mode = "off" if writes else "on"
    with psycopg.connect(url, autocommit=True, connect_timeout=15,
                         options=f"-c default_transaction_read_only={mode} -c statement_timeout=120000 -c lock_timeout=3000") as conn:
        if args.phase != "status":
            acquire_worker_lock(conn, args.schema)
        if args.phase == "init":
            initialize(conn, args.schema)
        elif args.phase == "catch-up":
            catch_up(conn, args.schema)
        elif args.phase == "cutover":
            cutover(conn, args.schema, writers_paused=args.writers_paused)
        elif args.phase == "rollback":
            rollback(conn, args.schema, writers_paused=args.writers_paused)
        elif args.phase in ("inventory", "build"):
            bucket = os.getenv("BUCKET")
            if not bucket:
                parser.error("BUCKET is required.")
            client = get_bucket_client(max_pool_connections=args.workers)
            if args.phase == "inventory":
                inventory(conn, client, bucket, args.schema, recent=args.recent)
            else:
                build(conn, client, bucket, args.schema, batch_size=args.batch_size,
                      workers=args.workers, limit=args.limit, retry_errors=args.retry_errors)
        elif args.phase == "validate":
            report = validation_report(conn, args.schema)
            print(json.dumps(report, indent=2, default=str), flush=True)
            if not report["ready_for_snapshot"]:
                raise SystemExit(2)
        else:
            verify_database(conn, args.schema, allow_complete=True)
            print("File states:", conn.execute(sql.SQL("SELECT status,COUNT(*),COALESCE(SUM(row_count),0) FROM {} GROUP BY status").format(table(args.schema, "files"))).fetchall())
            print("State:", conn.execute(sql.SQL("SELECT cutoff,created_at,inventory_at,validated_at,completed_at FROM {} WHERE singleton").format(table(args.schema, "state"))).fetchone())


if __name__ == "__main__":
    main()