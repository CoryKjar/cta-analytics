import os
from pathlib import Path

import boto3
import psycopg
from botocore.config import Config
from dotenv import load_dotenv

load_dotenv()

BUCKET_PREFIX = "raw/"
MISSING_LIST = Path("data/missing_bucket_files.txt")
LOCAL_DIR = Path("data/raw/arrivals")


def get_clients():
    client = boto3.client(
        "s3",
        endpoint_url=os.getenv("ENDPOINT"),
        aws_access_key_id=os.getenv("ACCESS_KEY_ID"),
        aws_secret_access_key=os.getenv("SECRET_ACCESS_KEY"),
        region_name=os.getenv("REGION", "auto"),
        config=Config(s3={"addressing_style": "virtual"}),
    )
    bucket = os.getenv("BUCKET")
    database_url = os.getenv("DATABASE_URL")
    if not bucket or not database_url:
        raise ValueError("BUCKET and DATABASE_URL must be configured.")
    return client, bucket, database_url


def get_registered(database_url):
    with psycopg.connect(database_url, connect_timeout=15) as conn:
        return {
            row[0]
            for row in conn.execute(
                "SELECT source_file FROM ingestion_files"
            ).fetchall()
        }


def list_bucket_xml(client, bucket):
    """Return XML object keys and unique basenames from one inventory pass."""
    keys = []
    names = set()

    paginator = client.get_paginator("list_objects_v2")
    for page in paginator.paginate(Bucket=bucket, Prefix=BUCKET_PREFIX):
        for obj in page.get("Contents", []):
            key = obj["Key"]
            if key.lower().endswith(".xml"):
                keys.append(key)
                names.add(key.rsplit("/", 1)[-1])

    return keys, names


def main():
    client, bucket, database_url = get_clients()

    print("Listing Bucket XML objects...", flush=True)
    object_keys, bucket_names = list_bucket_xml(client, bucket)

    # Read the manifest after the inventory scan, reducing false positives
    # caused by files registered while the Bucket was being listed.
    print("Reading PostgreSQL manifest...", flush=True)
    registered = get_registered(database_url)

    candidate_names = bucket_names - registered

    # Recheck candidates against PostgreSQL once more immediately before
    # reporting them, because the live collector may have registered some.
    if candidate_names:
        print(
            f"Rechecking {len(candidate_names):,} candidate filenames...",
            flush=True,
        )
        latest_registered = get_registered(database_url)
        missing_names = candidate_names - latest_registered
    else:
        latest_registered = registered
        missing_names = set()

    # Keep all Bucket keys for a missing basename in the list, if duplicate
    # basenames ever appear under different prefixes.
    missing_keys = sorted(
        key for key in object_keys
        if key.rsplit("/", 1)[-1] in missing_names
    )

    MISSING_LIST.parent.mkdir(parents=True, exist_ok=True)
    MISSING_LIST.write_text(
        "".join(f"{key}\n" for key in missing_keys),
        encoding="utf-8",
    )

    local_names = {path.name for path in LOCAL_DIR.glob("*.xml")}
    already_local = missing_names & local_names
    not_local = missing_names - local_names

    print("\nReconciliation summary")
    print(f"Bucket XML objects: {len(object_keys):,}")
    print(f"Unique Bucket XML filenames: {len(bucket_names):,}")
    print(f"Registered unique database filenames: {len(latest_registered):,}")
    print(f"Missing unique filenames after recheck: {len(missing_names):,}")
    print(f"Missing Bucket keys written to {MISSING_LIST}: {len(missing_keys):,}")
    print(f"Missing files already local: {len(already_local):,}")
    print(f"Missing files not local: {len(not_local):,}")

    if missing_names:
        print(
            "\nNote: collection is active, so this is still a point-in-time "
            "reconciliation, not a transactionally consistent snapshot."
        )
    else:
        print("\nNo unregistered Bucket XML filenames found in this scan.")


if __name__ == "__main__":
    main()
