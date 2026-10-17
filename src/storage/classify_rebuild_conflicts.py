"""Read-only classification of rebuild source conflicts; never resolves or loads them."""
import argparse
import hashlib
import json
import os
from collections import Counter, defaultdict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timezone
from pathlib import Path
import xml.etree.ElementTree as ET

import psycopg
from dotenv import load_dotenv

from src.storage.object_storage import get_bucket_client
from src.storage.rebuild_arrivals import acquire_worker_lock, table, verify_database
from src.storage.restore_train_identity import COLUMNS, source_prediction_records


IDENTITY_INDEXES = (0, 2, 3, 8, 15)
TELEMETRY_FIELDS = {"latitude", "longitude", "heading"}
FLAG_FIELDS = {"is_approaching", "is_delayed"}
REVISION_FIELDS = {"prediction_time", "wait_time"}
PRIORITY = ("metadata_conflict", "flag_conflict", "prediction_revision", "telemetry_only", "exact_duplicate")


def classify_differences(fields):
    fields = set(fields)
    if not fields:
        return ["exact_duplicate"]
    categories = []
    if fields & TELEMETRY_FIELDS:
        categories.append("telemetry_only" if fields <= TELEMETRY_FIELDS else "telemetry_difference")
    if fields & FLAG_FIELDS:
        categories.append("flag_conflict")
    if fields & REVISION_FIELDS:
        categories.append("prediction_revision")
    if fields - TELEMETRY_FIELDS - FLAG_FIELDS - REVISION_FIELDS:
        categories.append("metadata_conflict")
    return categories


def classify_source(xml_bytes, record, routes, destinations):
    file_id, filename, collected, old_count, key, etag = record
    rows = source_prediction_records(xml_bytes, record[:4], routes, destinations)
    etas = ET.fromstring(xml_bytes).findall("eta")
    groups = defaultdict(list)
    for position, (row, eta) in enumerate(zip(rows, etas), 1):
        groups[tuple(row[index] for index in IDENTITY_INDEXES)].append(
            (position, row, {child.tag: child.text for child in eta})
        )
    collisions = []
    categories = set()
    for identity, entries in groups.items():
        if len(entries) < 2:
            continue
        differences = {
            name: [{"xml_entry": position, "value": row[index]} for position, row, raw in entries]
            for index, name in enumerate(COLUMNS)
            if any(row[index] != entries[0][1][index] for position, row, raw in entries[1:])
        }
        kinds = classify_differences(differences)
        categories.update(kinds)
        raw_fields = sorted(set().union(*(raw for position, row, raw in entries)))
        raw_differences = {
            field: [{"xml_entry": position, "value": raw.get(field)} for position, row, raw in entries]
            for field in raw_fields
            if any(raw.get(field) != entries[0][2].get(field) for position, row, raw in entries[1:])
        }
        collisions.append({
            "identity": dict(zip((COLUMNS[index] for index in IDENTITY_INDEXES), identity)),
            "xml_entries": [entry[0] for entry in entries], "categories": kinds,
            "different_fields": sorted(differences), "normalized_differences": differences,
            "raw_field_differences": raw_differences,
            "variants": [{"xml_entry": position, "raw_fields": raw,
                          "normalized_fields": dict(zip(COLUMNS, row))} for position, row, raw in entries],
        })
    conflicts = categories - {"exact_duplicate", "telemetry_difference"}
    if not collisions:
        primary = "no_collision_reproduced"
    else:
        primary = next((kind for kind in PRIORITY if kind in conflicts), "exact_duplicate")
    return {
        "file_id": file_id, "source_file": filename, "object_key": key, "etag": etag,
        "source_sha256": hashlib.sha256(xml_bytes).hexdigest(), "xml_entries": len(rows),
        "distinct_run_identities": len(groups), "primary_category": primary,
        "file_categories": sorted(categories), "groups": collisions,
    }


def classification_summary(results, schema):
    primary, files_by_category, groups_by_category, patterns, fields = (Counter() for index in range(5))
    conflict_groups = exact_groups = read_errors = 0
    for result in results:
        primary[result["primary_category"]] += 1
        files_by_category.update(set(result.get("file_categories", [])))
        read_errors += int(result["primary_category"] == "unreadable_source")
        for group in result.get("groups", []):
            groups_by_category.update(set(group["categories"]))
            if group["different_fields"]:
                conflict_groups += 1
                patterns[", ".join(group["different_fields"])] += 1
                fields.update(group["different_fields"])
            else:
                exact_groups += 1
    return {
        "created_at_utc": datetime.now(timezone.utc).isoformat(), "schema": schema,
        "mode": "read-only classification; no resolution policy applied",
        "files_examined": len(results), "primary_file_categories": dict(primary),
        "files_by_category_nonexclusive": dict(files_by_category),
        "groups_by_category_nonexclusive": dict(groups_by_category),
        "conflicting_groups": conflict_groups, "exact_duplicate_groups": exact_groups,
        "unreadable_sources": read_errors, "field_patterns": dict(patterns.most_common()),
        "field_counts": dict(fields.most_common()),
    }


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--schema", required=True)
    parser.add_argument("--workers", type=int, default=16)
    parser.add_argument("--public-url", action="store_true")
    parser.add_argument("--output-dir", required=True, help="New local directory for JSONL evidence and JSON summary")
    args = parser.parse_args()
    if not 1 <= args.workers <= 32:
        parser.error("Workers must be 1..32.")
    output = Path(args.output_dir)
    if output.exists():
        parser.error("Output directory already exists; choose a new path to preserve prior reports.")
    load_dotenv()
    url = os.getenv("DATABASE_PUBLIC_URL" if args.public_url else "DATABASE_URL")
    bucket = os.getenv("BUCKET")
    if not url or not bucket:
        parser.error("The selected database URL and BUCKET must be configured.")
    with psycopg.connect(url, autocommit=True, connect_timeout=15,
                         options='-c default_transaction_read_only=on -c statement_timeout=30000 -c lock_timeout=2000') as conn:
        acquire_worker_lock(conn, args.schema)
        verify_database(conn, args.schema)
        records = conn.execute(psycopg.sql.SQL("""
            SELECT file_id,source_file,collection_timestamp,snapshot_row_count,object_key,etag
            FROM {} WHERE status='error' ORDER BY file_id
        """).format(table(args.schema, "files"))).fetchall()
        routes = {name: identity for identity, name in conn.execute("SELECT route_id,route_name FROM public.routes").fetchall()}
        destinations = {name: identity for identity, name in conn.execute("SELECT destination_id,destination_name FROM public.destinations").fetchall()}
        client = get_bucket_client(max_pool_connections=args.workers)

        def read(record):
            response = None
            try:
                if not record[4] or not record[5]:
                    raise ValueError("Missing inventoried source key/ETag.")
                response = client.get_object(Bucket=bucket, Key=record[4], IfMatch=record[5])
                return classify_source(response["Body"].read(), record, routes, destinations)
            except Exception as exc:
                return {"file_id": record[0], "source_file": record[1], "object_key": record[4],
                        "primary_category": "unreadable_source", "file_categories": ["unreadable_source"],
                        "error": str(exc)[:500] if isinstance(exc, ValueError) else type(exc).__name__, "groups": []}
            finally:
                if response is not None:
                    response["Body"].close()

        output.mkdir(parents=True)
        results = []
        with (output / "details.jsonl").open("x", encoding="utf-8") as report, ThreadPoolExecutor(max_workers=args.workers) as executor:
            for result in executor.map(read, records):
                report.write(json.dumps(result, default=str, allow_nan=False) + "\n")
                results.append(result)
                if len(results) % 250 == 0:
                    print(f"Classified: {len(results)}/{len(records)} files", flush=True)
        summary = classification_summary(results, args.schema)
        with (output / "summary.json").open("x", encoding="utf-8") as report:
            json.dump(summary, report, indent=2)
        print(json.dumps(summary, indent=2), flush=True)
        print("Evidence directory:", output, flush=True)
        if summary["unreadable_sources"]:
            raise SystemExit(2)


if __name__ == "__main__":
    main()