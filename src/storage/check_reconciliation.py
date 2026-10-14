"""Read-only Bucket-to-manifest and row-count integrity report."""
import os
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()
PREFIX = "raw/"
REPORT = Path("data/reconciliation_report.txt")


def main():
    bucket = os.getenv("BUCKET")
    database_url = os.getenv("DATABASE_URL")
    required = ["ENDPOINT", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"]
    missing = [key for key in required if not os.getenv(key)]
    if not bucket or not database_url or missing:
        raise ValueError(f"Missing configuration: BUCKET/DATABASE_URL or {missing}")

    client = boto3.client(
        "s3", endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION", "auto"),
        config=Config(s3={"addressing_style": "virtual"}),
    )

    objects = {}
    duplicate_basenames = {}
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=bucket, Prefix=PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".xml"):
                name = key.rsplit("/", 1)[-1]
                if name in objects and objects[name] != key:
                    duplicate_basenames.setdefault(name, [objects[name]]).append(key)
                else:
                    objects[name] = key

    with psycopg.connect(database_url, connect_timeout=20) as conn:
        manifest_rows = conn.execute(
            "SELECT source_file, status, COALESCE(row_count,0), attempts, last_error "
            "FROM ingestion_files"
        ).fetchall()
        missing_fk_count = conn.execute("""
            SELECT COUNT(*) FROM arrivals a
            LEFT JOIN ingestion_files f ON f.file_id = a.file_id
            WHERE f.file_id IS NULL
        """).fetchone()[0]
        row_mismatches = conn.execute("""
            SELECT f.source_file, f.status, f.row_count, COUNT(a.*)::bigint AS actual_rows
            FROM ingestion_files f
            LEFT JOIN arrivals a ON a.file_id = f.file_id
            WHERE f.status = 'processed'
            GROUP BY f.source_file, f.status, f.row_count
            HAVING COALESCE(f.row_count,0) <> COUNT(a.*)
            ORDER BY f.source_file
        """).fetchall()

    manifest = {r[0]: r for r in manifest_rows}
    bucket_names = set(objects)
    registered = set(manifest)
    missing_manifest = sorted(bucket_names - registered)
    no_bucket_object = sorted(registered - bucket_names)
    unhealthy = sorted((name, row[1], row[3], row[4]) for name, row in manifest.items()
                       if row[1] != "processed")
    report = [
        "CTA PIPELINE RECONCILIATION REPORT",
        "Note: Bucket and database inventories are not an atomic snapshot while collection is active.",
        f"Bucket XML objects (unique basenames): {len(bucket_names):,}",
        f"Manifest rows (unique source_file): {len(registered):,}",
        f"Bucket XML missing manifest row: {len(missing_manifest):,}",
        f"Manifest rows with no matching Bucket basename: {len(no_bucket_object):,}",
        f"Non-processed manifest rows: {len(unhealthy):,}",
        f"Processed row_count mismatches: {len(row_mismatches):,}",
        f"Arrivals without manifest FK: {missing_fk_count:,}",
        f"Duplicate Bucket basenames: {len(duplicate_basenames):,}",
        "",
        "MISSING MANIFEST:",
        *missing_manifest,
        "",
        "MANIFEST WITHOUT BUCKET OBJECT:",
        *no_bucket_object,
        "",
        "UNHEALTHY MANIFEST ROWS (filename | status | attempts | last_error):",
        *(f"{n} | {s} | {a} | {e}" for n, s, a, e in unhealthy),
        "",
        "ROW COUNT MISMATCHES (filename | status | recorded | actual):",
        *(f"{n} | {s} | {recorded} | {actual}" for n, s, recorded, actual in row_mismatches),
        "",
        "DUPLICATE BASENAMES:",
        *(f"{name}: {', '.join(keys)}" for name, keys in sorted(duplicate_basenames.items())),
    ]
    REPORT.parent.mkdir(parents=True, exist_ok=True)
    REPORT.write_text("\n".join(report) + "\n", encoding="utf-8")
    print("\n".join(report[:11]))
    print(f"\\nFull report written to {REPORT}")


if __name__ == "__main__":
    main()
