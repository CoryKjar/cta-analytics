from datetime import datetime
from pathlib import Path

import pandas as pd

from src.ingestion.fetch_arrivals import fetch_arrivals
from src.transform.parse_arrivals import parse_arrivals
from src.storage.load_arrivals_postgres import (
    load_arrivals_data,
    register_file_results,
)
from src.utils.archive import archive_files


def _timestamp_from_path(path):
    parts = Path(path).stem.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected XML filename: {Path(path).name}")
    return datetime.strptime(
        f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S"
    )


def main():
    fetched_files = fetch_arrivals()
    if not fetched_files:
        print("No station snapshots were uploaded; nothing to parse.")
        return

    parsed_frames = []
    empty_files = []
    parse_failures = []

    for path in fetched_files:
        path = Path(path)
        try:
            df = parse_arrivals(path)
            if df.empty:
                empty_files.append({
                    "source_file": path.name,
                    "collection_timestamp": _timestamp_from_path(path),
                    "station_id": None,
                    "status": "processed",
                    "row_count": 0,
                    "last_error": None,
                })
            else:
                parsed_frames.append(df)
        except Exception as exc:
            parse_failures.append({
                "source_file": path.name,
                "collection_timestamp": _timestamp_from_path(path),
                "station_id": None,
                "status": "failed",
                "row_count": 0,
                "last_error": str(exc)[:4000],
            })
            print(f"Parse failed for {path.name}: {exc}")

    # Load arrival-bearing files. The loader marks these files processed
    # transactionally with their arrival inserts.
    successfully_loaded_files = set()
    if parsed_frames:
        all_rows = pd.concat(parsed_frames, ignore_index=True)
        load_arrivals_data(all_rows)
        successfully_loaded_files = set(all_rows["source_file"].unique())

    # Register valid empty XML snapshots as processed, in one transaction.
    successfully_empty_files = set()
    if empty_files:
        register_file_results(empty_files)
        successfully_empty_files = {item["source_file"] for item in empty_files}

    # Record parse failures, but retain their local XML for future investigation/retry.
    if parse_failures:
        register_file_results(parse_failures)

    # Delete local staging copies only after successful load/registration.
    safe_to_archive = [
        path for path in fetched_files
        if Path(path).name in successfully_loaded_files
        or Path(path).name in successfully_empty_files
    ]
    if safe_to_archive:
        archive_files(safe_to_archive)

    print("\nPipeline summary")
    print(f"Uploaded files: {len(fetched_files):,}")
    print(f"Files loaded with arrivals: {len(successfully_loaded_files):,}")
    print(f"Valid zero-arrival files registered: {len(successfully_empty_files):,}")
    print(f"Parse failures retained locally: {len(parse_failures):,}")
    print(f"Files archived: {len(safe_to_archive):,}")


if __name__ == "__main__":
    main()
