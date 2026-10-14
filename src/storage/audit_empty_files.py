
import os
import xml.etree.ElementTree as ET
from pathlib import Path

import psycopg
from dotenv import load_dotenv

load_dotenv()

STAGING_DIR = Path("data/raw/arrivals")


def main():
    with psycopg.connect(os.environ["DATABASE_URL"]) as conn:
        rows = conn.execute("""
            SELECT f.source_file
            FROM ingestion_files f
            WHERE NOT EXISTS (
                SELECT 1
                FROM arrivals a
                WHERE a.file_id = f.file_id
            )
        """).fetchall()

    checked = empty = with_arrivals = missing = errors = 0

    for (filename,) in rows:
        path = STAGING_DIR / filename

        if not path.exists():
            missing += 1
            continue

        try:
            count = len(ET.parse(path).getroot().findall("eta"))
            checked += 1

            if count == 0:
                empty += 1
            else:
                with_arrivals += 1
                print(f"HAS ARRIVALS: {filename} ({count} records)")

        except Exception as exc:
            errors += 1
            print(f"XML ERROR: {filename}: {exc}")

    print("\nAudit summary")
    print(f"Manifest files without arrivals: {len(rows):,}")
    print(f"Local files checked: {checked:,}")
    print(f"Confirmed empty XML files: {empty:,}")
    print(f"XML files containing arrivals: {with_arrivals:,}")
    print(f"Files missing locally: {missing:,}")
    print(f"XML parse errors: {errors:,}")


if __name__ == "__main__":
    main()
