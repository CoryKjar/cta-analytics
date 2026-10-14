"""

Bounded, idempotent recovery of Bucket XML files missing from or unhealthy in the manifest.



Run manually:

    python -m src.storage.self_heal



Optional environment:

    SELF_HEAL_BATCH_SIZE=100

    SELF_HEAL_MAX_FILES=500

    SELF_HEAL_MAX_ATTEMPTS=5

    SELF_HEAL_DRY_RUN=true



This scans the Bucket inventory, but downloads/processes only bounded candidates.

Schedule daily/weekly rather than running on every frequent collector cycle.

"""

import os

import re

from datetime import datetime

from pathlib import Path



import boto3

import pandas as pd

import psycopg

from botocore.config import Config

from dotenv import load_dotenv



from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results

from src.transform.parse_arrivals import parse_arrivals

from src.utils.archive import archive_files



load_dotenv()

PREFIX = "raw/"

STAGING_DIR = Path("data/raw/arrivals")

BATCH_SIZE = max(1, int(os.getenv("SELF_HEAL_BATCH_SIZE", "100")))

MAX_FILES = max(1, int(os.getenv("SELF_HEAL_MAX_FILES", "500")))

MAX_ATTEMPTS = max(1, int(os.getenv("SELF_HEAL_MAX_ATTEMPTS", "5")))

DRY_RUN = os.getenv("SELF_HEAL_DRY_RUN", "false").lower() in {"1", "true", "yes"}





def _clients():

    database_url = os.getenv("DATABASE_URL")

    bucket = os.getenv("BUCKET")

    required = ["ENDPOINT", "ACCESS_KEY_ID", "SECRET_ACCESS_KEY"]

    missing = [name for name in required if not os.getenv(name)]

    if not database_url or not bucket or missing:

        raise ValueError(f"Missing configuration: DATABASE_URL/BUCKET or {missing}")

    client = boto3.client(

        "s3",

        endpoint_url=os.getenv("ENDPOINT"),

        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),

        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),

        region_name=os.getenv("REGION", "auto"),

        config=Config(s3={"addressing_style": "virtual"}),

    )

    return client, bucket, database_url





def _timestamp(name):

    match = re.match(r"^(\d{8})_(\d{6})_.+\.xml$", name)

    if not match:

        raise ValueError(f"Unrecognized snapshot filename: {name}")

    return datetime.strptime(f"{match.group(1)}_{match.group(2)}", "%Y%m%d_%H%M%S")





def _manifest(database_url):

    with psycopg.connect(database_url, connect_timeout=20) as conn:

        rows = conn.execute(

            "SELECT source_file, status, COALESCE(row_count, 0), attempts "

            "FROM ingestion_files"

        ).fetchall()

        counts = conn.execute(

            """

            SELECT f.source_file, f.row_count, COUNT(a.*)::bigint

            FROM ingestion_files f

            LEFT JOIN arrivals a ON a.file_id = f.file_id

            WHERE f.status = 'processed'

            GROUP BY f.source_file, f.row_count

            HAVING COALESCE(f.row_count, 0) <> COUNT(a.*)

            """

        ).fetchall()

    return {name: {"status": status, "row_count": row_count, "attempts": attempts}

            for name, status, row_count, attempts in rows}, counts





def _inventory(client, bucket):

    # Preserve full keys so recovery downloads the exact object, not a guessed path.

    objects = {}

    paginator = client.get_paginator("list_objects_v2")

    for page in paginator.paginate(Bucket=bucket, Prefix=PREFIX):

        for obj in page.get("Contents", []):

            key = obj["Key"]

            if key.lower().endswith(".xml"):

                name = key.rsplit("/", 1)[-1]

                # Filename is the current schema's manifest key. If duplicate

                # basenames exist in different folders, stop rather than guess.

                if name in objects and objects[name] != key:

                    raise RuntimeError(

                        f"Duplicate XML basename {name!r}: {objects[name]!r}, {key!r}"

                    )

                objects[name] = key

    return objects





def _repair_counts(database_url):

    # Only repairs the manifest's derived row_count from committed fact rows.

    # Does not modify arrivals or invent processing timestamps.

    with psycopg.connect(database_url, connect_timeout=20) as conn:

        with conn.cursor() as cur:

            cur.execute("""

                UPDATE ingestion_files f

                SET row_count = x.actual_count, updated_at = NOW()

                FROM (

                    SELECT f2.file_id, COUNT(a.*)::integer AS actual_count

                    FROM ingestion_files f2

                    LEFT JOIN arrivals a ON a.file_id = f2.file_id

                    WHERE f2.status = 'processed'

                    GROUP BY f2.file_id

                ) x

                WHERE f.file_id = x.file_id

                  AND f.status = 'processed'

                  AND COALESCE(f.row_count, 0) <> x.actual_count

                RETURNING f.source_file

            """)

            fixed = [r[0] for r in cur.fetchall()]

    return fixed





def run_self_heal():

    client, bucket, database_url = _clients()

    print("Self-heal: inventorying Bucket XML objects...", flush=True)

    objects = _inventory(client, bucket)

    manifest, count_mismatches = _manifest(database_url)



    # Candidates: absent from manifest, non-processed, or failed too many times

    # are excluded so poison files do not loop forever.

    candidates = []

    for name, key in objects.items():

        row = manifest.get(name)

        if row is None:

            candidates.append((name, key, "missing_manifest"))

        elif row["status"] != "processed" and row["attempts"] < MAX_ATTEMPTS:

            candidates.append((name, key, f"status_{row['status']}"))

    candidates.sort(key=lambda item: (item[2] != "missing_manifest", item[0]))

    candidates = candidates[:MAX_FILES]



    print(f"Bucket XML objects: {len(objects):,}", flush=True)

    print(f"Manifest rows: {len(manifest):,}", flush=True)

    print(f"Processed row-count mismatches found: {len(count_mismatches):,}", flush=True)

    print(f"Recovery candidates (bounded to {MAX_FILES}): {len(candidates):,}", flush=True)



    if DRY_RUN:

        for name, _, reason in candidates[:50]:

            print(f"DRY RUN candidate [{reason}]: {name}")

        print("Dry run enabled; no data changed.")

        return {"candidates": len(candidates), "recovered": 0, "dry_run": True}



    repaired = _repair_counts(database_url)

    if repaired:

        print(f"Repaired {len(repaired):,} derived row_count values.", flush=True)



    stats = {"recovered_with_arrivals": 0, "recovered_empty": 0,

             "parse_failures": 0, "download_failures": 0, "load_failures": 0}

    STAGING_DIR.mkdir(parents=True, exist_ok=True)



    for start in range(0, len(candidates), BATCH_SIZE):

        batch = candidates[start:start + BATCH_SIZE]

        frames, empty, failures, downloaded_paths = [], [], [], []

        for name, key, _reason in batch:

            path = STAGING_DIR / name

            try:

                # Always refresh the local copy from the durable Bucket object.

                client.download_file(bucket, key, str(path))

                downloaded_paths.append(path)

            except Exception as exc:

                stats["download_failures"] += 1

                failures.append((name, key, f"download: {exc}"))

                continue

            try:

                df = parse_arrivals(path)

                if df.empty:

                    empty.append({

                        "source_file": name, "collection_timestamp": _timestamp(name),

                        "station_id": None, "status": "processed", "row_count": 0,

                        "last_error": None,

                    })

                else:

                    frames.append(df)

            except Exception as exc:

                stats["parse_failures"] += 1

                failures.append((name, key, f"parse: {exc}"))





        # Track only files whose processing was confirmed successful.
        good_names = set()

        # Load non-empty files independently from empty-file registration.
        if frames:
            frame_names = {
                str(name)
                for frame in frames
                for name in frame["source_file"].dropna().unique()
            }
            try:
                combined = pd.concat(frames, ignore_index=True)
                load_arrivals_data(combined)
                stats["recovered_with_arrivals"] += len(frame_names)
                good_names.update(frame_names)
            except Exception as exc:
                stats["load_failures"] += len(frame_names)
                print(
                    f"LOAD ERROR in batch {start // BATCH_SIZE + 1}: {exc}",
                    flush=True,
                )

                failure_records = []
                for name in sorted(frame_names):
                    try:
                        failure_records.append({
                            "source_file": name,
                            "collection_timestamp": _timestamp(name),
                            "station_id": None,
                            "status": "failed",
                            "row_count": 0,
                            "last_error": f"load: {exc}"[:4000],
                        })
                    except ValueError:
                        print(
                            f"Could not parse timestamp from failed file: {name}",
                            flush=True,
                        )

                if failure_records:
                    try:
                        register_file_results(failure_records)
                    except Exception as record_exc:
                        print(
                            f"Could not record load failures: {record_exc}",
                            flush=True,
                        )

        # Register empty files separately, so their failure cannot affect
        # files successfully loaded by load_arrivals_data().
        if empty:
            empty_names = {item["source_file"] for item in empty}
            try:
                register_file_results(empty)
                stats["recovered_empty"] += len(empty)
                good_names.update(empty_names)
            except Exception as exc:
                stats["load_failures"] += len(empty_names)
                print(
                    f"EMPTY-FILE REGISTRATION ERROR in batch "
                    f"{start // BATCH_SIZE + 1}: {exc}",
                    flush=True,
                )

                failure_records = []
                for name in sorted(empty_names):
                    try:
                        failure_records.append({
                            "source_file": name,
                            "collection_timestamp": _timestamp(name),
                            "station_id": None,
                            "status": "failed",
                            "row_count": 0,
                            "last_error": f"empty-file registration: {exc}"[:4000],
                        })
                    except ValueError:
                        print(
                            f"Could not parse timestamp from failed file: {name}",
                            flush=True,
                        )

                if failure_records:
                    try:
                        register_file_results(failure_records)
                    except Exception as record_exc:
                        print(
                            f"Could not record empty-file failures: {record_exc}",
                            flush=True,
                        )

        # Record download and parse failures independently.
        if failures:
            failure_records = []
            for name, _key, error in failures:
                try:
                    ts = _timestamp(name)
                except ValueError:
                    print(
                        f"Could not parse timestamp from failed file: {name}",
                        flush=True,
                    )
                    continue

                failure_records.append({
                    "source_file": name,
                    "collection_timestamp": ts,
                    "station_id": None,
                    "status": "failed",
                    "row_count": 0,
                    "last_error": error[:4000],
                })

            if failure_records:
                try:
                    register_file_results(failure_records)
                except Exception as exc:
                    print(f"Could not record failures: {exc}", flush=True)

        # Archive only files successfully loaded or registered as empty.
        good_paths = [
            path for path in downloaded_paths
            if path.name in good_names
        ]
        if good_paths:
            archive_files(good_paths)

    print("\nSELF-HEAL SUMMARY")


    for key, value in stats.items():

        print(f"{key}: {value:,}")

    print(f"Row-count values repaired: {len(repaired):,}")

    print(f"Unprocessed candidates skipped due to attempt cap: "

          f"{sum(1 for n, r in manifest.items() if r['status'] != 'processed' and r['attempts'] >= MAX_ATTEMPTS):,}")

    return {"candidates": len(candidates), **stats, "row_counts_repaired": len(repaired)}





if __name__ == "__main__":

    run_self_heal()
