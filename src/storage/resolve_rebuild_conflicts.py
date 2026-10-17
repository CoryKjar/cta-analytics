"""Apply the approved audited unknown-value policy to rebuild staging only."""
import argparse
import gzip
import hashlib
import json
import math
import os
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path

import psycopg
from psycopg import sql
from psycopg.types.json import Jsonb
from dotenv import load_dotenv

from src.storage.object_storage import get_bucket_client
from src.storage.restore_train_identity import COLUMNS, source_prediction_records


POLICY = "unknown_conflicts_v1"
IDENTITY_INDEXES = (0, 2, 3, 8, 15)
ALLOWED_FIELDS = {"heading", "latitude", "longitude", "is_delayed"}


def resolve_unknowns(rows):
    """Only agreed values survive; disputed GPS coordinates are unknown as a pair."""
    groups = defaultdict(list)
    for position, row in enumerate(rows, 1):
        if len(row) != len(COLUMNS):
            raise ValueError("Unexpected source record shape.")
        groups[tuple(row[index] for index in IDENTITY_INDEXES)].append((position, row))
    canonical, decisions = [], []
    for identity, variants in groups.items():
        differences = {name for index, name in enumerate(COLUMNS)
                       if any(row[index] != variants[0][1][index] for position, row in variants[1:])}
        if differences - ALLOWED_FIELDS:
            raise ValueError("Source differences exceed the approved staging policy: " + ", ".join(sorted(differences - ALLOWED_FIELDS)))
        null_fields = set(differences)
        if null_fields & {"latitude", "longitude"}:
            null_fields.update(("latitude", "longitude"))
        resolved = list(variants[0][1])
        for name in null_fields:
            resolved[COLUMNS.index(name)] = None
        canonical.append(tuple(resolved))
        if differences:
            decisions.append({
                "identity": dict(zip((COLUMNS[index] for index in IDENTITY_INDEXES), identity)),
                "xml_entries": [position for position, row in variants],
                "different_fields": sorted(differences), "null_fields": sorted(null_fields),
                "reason": "Conflicting source values are unknown; no variant was selected.",
                "normalized_variants": [dict(zip(COLUMNS, row)) for position, row in variants],
            })
    return canonical, decisions


def canonical_rows_match(actual, expected):
    if len(actual) != len(expected):
        return False
    indexed = {tuple(row[index] for index in IDENTITY_INDEXES): row for row in actual}
    if len(indexed) != len(actual):
        return False
    for source in expected:
        stored = indexed.get(tuple(source[index] for index in IDENTITY_INDEXES))
        if stored is None:
            return False
        for index, value in enumerate(source):
            if index in (11, 12, 14) and stored[index] is not None and value is not None:
                if not math.isclose(stored[index], value, rel_tol=0, abs_tol=0.00001):
                    return False
            elif stored[index] != value:
                return False
    return True


def create_audit_table(conn, schema):
    from src.storage.rebuild_arrivals import table
    conn.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            file_id integer PRIMARY KEY REFERENCES {}(file_id),
            policy text NOT NULL, source_sha256 text NOT NULL, source_etag text NOT NULL,
            source_xml_gzip bytea NOT NULL, evidence jsonb NOT NULL,
            canonical_row_count integer NOT NULL, resolved_at timestamptz NOT NULL DEFAULT NOW()
        )
    """).format(table(schema, "resolution_audit"), table(schema, "files")))


def apply_resolution_batch(conn, schema, plans):
    from src.storage.rebuild_arrivals import copy_rows, table
    if not plans:
        return
    ids = [plan["record"][0] for plan in plans]
    with conn.transaction():
        locked = conn.execute(sql.SQL("""
            SELECT file_id,source_file,collection_timestamp,snapshot_row_count,object_key,etag
            FROM {} WHERE file_id=ANY(%s) AND status='error' ORDER BY file_id FOR UPDATE
        """).format(table(schema, "files")), (ids,)).fetchall()
        if sorted(locked) != sorted(plan["record"] for plan in plans):
            raise RuntimeError("Staging file state changed; resolution batch rolled back.")
        existing = conn.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE file_id=ANY(%s)").format(table(schema, "arrivals")), (ids,)).fetchone()[0]
        audits = conn.execute(sql.SQL("SELECT COUNT(*) FROM {} WHERE file_id=ANY(%s)").format(table(schema, "resolution_audit")), (ids,)).fetchone()[0]
        if existing or audits:
            raise RuntimeError("Staged rows or prior audits already exist; refusing to overwrite them.")
        with conn.cursor() as cursor:
            for plan in plans:
                record = plan["record"]
                evidence = {"policy": POLICY, "source_file": record[1], "object_key": record[4],
                            "decisions": plan["decisions"]}
                cursor.execute(sql.SQL("""
                    INSERT INTO {} (file_id,policy,source_sha256,source_etag,source_xml_gzip,evidence,canonical_row_count)
                    VALUES (%s,%s,%s,%s,%s,%s,%s)
                """).format(table(schema, "resolution_audit")), (
                    record[0], POLICY, plan["sha256"], record[5], gzip.compress(plan["xml"]),
                    Jsonb(evidence, dumps=lambda value: json.dumps(value, default=str, allow_nan=False)), len(plan["rows"]),
                ))
            copy_rows(cursor, table(schema, "arrivals"), COLUMNS,
                      (row for plan in plans for row in plan["rows"]))
            cursor.executemany(sql.SQL("""
                UPDATE {} SET status='loaded',row_count=%s,station_id=%s,source_sha256=%s,
                error=NULL,loaded_at=NOW() WHERE file_id=%s AND status='error'
            """).format(table(schema, "files")), [
                (len(plan["rows"]), plan["rows"][0][1], plan["sha256"], plan["record"][0]) for plan in plans
            ])
            if cursor.rowcount != len(plans):
                raise RuntimeError("Unexpected file update count; resolution batch rolled back.")
            cursor.execute(sql.SQL("UPDATE {} SET validated_at=NULL WHERE singleton").format(table(schema, "state")))


def audit_validation_report(conn, schema, *, live_arrivals=False):
    """Recompute every audited canonical row from its preserved original bytes."""
    from src.storage.rebuild_arrivals import table
    exists = conn.execute("SELECT to_regclass(%s)", (schema + ".resolution_audit",)).fetchone()[0]
    if not exists:
        return {"resolution_audit_files": 0, "resolution_audit_failures": 0}
    audits = conn.execute(sql.SQL("SELECT file_id,policy,source_sha256,source_etag,source_xml_gzip,evidence,canonical_row_count FROM {} ORDER BY file_id").format(table(schema, "resolution_audit"))).fetchall()
    if not audits:
        return {"resolution_audit_files": 0, "resolution_audit_failures": 0}
    ids = [row[0] for row in audits]
    files = {row[0]: row for row in conn.execute(sql.SQL("SELECT file_id,source_file,collection_timestamp,row_count,status,source_sha256,etag FROM {} WHERE file_id=ANY(%s)").format(table(schema, "files")), (ids,)).fetchall()}
    staged = defaultdict(list)
    cursor = conn.execute(sql.SQL("SELECT {} FROM {} WHERE file_id=ANY(%s)").format(
        sql.SQL(",").join(map(sql.Identifier, COLUMNS)),
        sql.Identifier("public", "arrivals") if live_arrivals else table(schema, "arrivals")), (ids,))
    for row in cursor.fetchall():
        staged[row[5]].append(row)
    routes = {name: identity for identity, name in conn.execute("SELECT route_id,route_name FROM public.routes").fetchall()}
    destinations = {name: identity for identity, name in conn.execute("SELECT destination_id,destination_name FROM public.destinations").fetchall()}
    failures = 0
    for file_id, policy, digest, etag, compressed, evidence, count in audits:
        record = files.get(file_id)
        try:
            if not record or policy != POLICY or record[4] != "loaded" or record[5:7] != (digest, etag):
                raise ValueError("Audit metadata differs from loaded source state.")
            data = gzip.decompress(compressed)
            if hashlib.sha256(data).hexdigest() != digest:
                raise ValueError("Preserved XML hash mismatch.")
            expected, decisions = resolve_unknowns(source_prediction_records(data, record[:4], routes, destinations))
            normalized = json.loads(json.dumps(decisions, default=str, allow_nan=False))
            if evidence.get("policy") != POLICY or evidence.get("decisions") != normalized:
                raise ValueError("Audit decisions do not match the original source.")
            if count != len(expected) or record[3] != count or not canonical_rows_match(staged[file_id], expected):
                raise ValueError("Staged rows differ from audited canonical rows.")
        except (ValueError, TypeError, OSError, EOFError):
            failures += 1
    return {"resolution_audit_files": len(audits), "resolution_audit_failures": failures}


def main():
    from src.storage.rebuild_arrivals import acquire_worker_lock, table, verify_database
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--classification-dir", required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--batch-size", type=int, default=100)
    parser.add_argument("--file-id", type=int, action="append", default=[])
    parser.add_argument("--public-url", action="store_true")
    parser.add_argument("--apply", action="store_true")
    args = parser.parse_args()
    if not 1 <= args.workers <= 32 or not 1 <= args.batch_size <= 250:
        parser.error("Workers must be 1..32; batch size must be 1..250.")
    evidence = {}
    with (Path(args.classification_dir) / "details.jsonl").open(encoding="utf-8") as source:
        for line in source:
            item = json.loads(line)
            if item["file_id"] in evidence or item["primary_category"] not in ("telemetry_only", "flag_conflict"):
                parser.error("Classification contains duplicate IDs or unapproved file categories.")
            evidence[item["file_id"]] = item
    ids = sorted(set(args.file_id) if args.file_id else evidence)
    if not ids or any(file_id not in evidence for file_id in ids):
        parser.error("Select classified conflict file IDs only.")
    load_dotenv()
    url = os.getenv("DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL")
    bucket = os.getenv("BUCKET")
    if not url or not bucket:
        parser.error("The selected database URL and BUCKET must be configured.")
    mode = "off" if args.apply else "on"
    with psycopg.connect(url, autocommit=True, connect_timeout=15,
                         options=f"-c default_transaction_read_only={mode} -c statement_timeout=120000 -c lock_timeout=3000") as conn:
        acquire_worker_lock(conn, args.schema)
        verify_database(conn, args.schema)
        files = conn.execute(sql.SQL("SELECT file_id,source_file,collection_timestamp,snapshot_row_count,object_key,etag FROM {} WHERE file_id=ANY(%s) AND status='error' ORDER BY file_id").format(table(args.schema, "files")), (ids,)).fetchall()
        routes = {name: identity for identity, name in conn.execute("SELECT route_id,route_name FROM public.routes").fetchall()}
        destinations = {name: identity for identity, name in conn.execute("SELECT destination_id,destination_name FROM public.destinations").fetchall()}
        client = get_bucket_client(max_pool_connections=args.workers)
        if args.apply:
            create_audit_table(conn, args.schema)
        print("Policy:", POLICY, "| Mode:", "STAGING APPLY" if args.apply else "READ-ONLY", "| Eligible files:", len(files), flush=True)

        def plan(record):
            prior = evidence[record[0]]
            if (prior["source_file"], prior["object_key"], prior["etag"]) != (record[1], record[4], record[5]):
                raise ValueError("Classified source metadata changed; refusing resolution.")
            response = client.get_object(Bucket=bucket, Key=record[4], IfMatch=record[5])
            try:
                data = response["Body"].read()
            finally:
                response["Body"].close()
            digest = hashlib.sha256(data).hexdigest()
            if digest != prior["source_sha256"]:
                raise ValueError("Classified source hash changed; refusing resolution.")
            rows, decisions = resolve_unknowns(source_prediction_records(data, record[:4], routes, destinations))
            if not rows or not decisions or len({row[1] for row in rows}) != 1:
                raise ValueError("Expected a nonempty single-station file with audited source conflicts.")
            return {"record": record, "xml": data, "sha256": digest, "rows": rows, "decisions": decisions}

        files_done = rows_done = groups_done = 0
        with ThreadPoolExecutor(max_workers=args.workers) as executor:
            for offset in range(0, len(files), args.batch_size):
                plans = list(executor.map(plan, files[offset:offset + args.batch_size]))
                if args.apply:
                    apply_resolution_batch(conn, args.schema, plans)
                files_done += len(plans)
                rows_done += sum(len(item["rows"]) for item in plans)
                groups_done += sum(len(item["decisions"]) for item in plans)
                print(f"Files: {files_done}/{len(files)} | Canonical rows: {rows_done} | Audited groups: {groups_done}", flush=True)
        print("Live tables and manifest history unchanged.", flush=True)
        if args.apply:
            report = audit_validation_report(conn, args.schema)
            print(json.dumps(report), flush=True)
            if report["resolution_audit_failures"]:
                raise SystemExit(2)


if __name__ == "__main__":
    main()