from datetime import datetime
from pathlib import Path
import os
import time
import pandas as pd

from src.ingestion.fetch_arrivals import fetch_arrivals
from src.transform.parse_arrivals import parse_arrivals
from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results
from src.utils.archive import archive_files
from src.utils.logger import get_logger

logger = get_logger(__name__)


def _timestamp_from_path(path):
    parts = Path(path).stem.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected XML filename: {Path(path).name}")
    return datetime.strptime(f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S")


def main():
    started = time.monotonic()
    logger.info("event=pipeline_started")

    if os.getenv("SELF_HEAL_ENABLED", "false").strip().lower() in {"1", "true", "yes"}:
        from src.storage.self_heal import run_self_heal
        logger.info("event=self_heal_started")
        try:
            result = run_self_heal()
            logger.info("event=self_heal_completed result=%s", result)
        except Exception as exc:
            # Keep current collection running even if optional recovery fails.
            logger.exception("event=self_heal_failed error_type=%s", type(exc).__name__)

    fetched_files = fetch_arrivals()
    if not fetched_files:
        logger.warning("event=pipeline_no_files_uploaded duration_seconds=%.2f", time.monotonic() - started)
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
                logger.info("event=file_parsed_zero_arrivals file=%s", path.name)
            else:
                parsed_frames.append(df)
                logger.info("event=file_parsed file=%s arrival_rows=%s", path.name, len(df))
        except Exception as exc:
            parse_failures.append({
                "source_file": path.name,
                "collection_timestamp": _timestamp_from_path(path),
                "station_id": None,
                "status": "failed",
                "row_count": 0,
                "last_error": str(exc)[:4000],
            })
            logger.exception("event=file_parse_failed file=%s error_type=%s", path.name, type(exc).__name__)

    successfully_loaded_files = set()
    loaded_rows = 0
    if parsed_frames:
        all_rows = pd.concat(parsed_frames, ignore_index=True)
        loaded_rows = len(all_rows)
        load_arrivals_data(all_rows)
        successfully_loaded_files = set(all_rows["source_file"].unique())
        logger.info("event=database_load_succeeded files=%s arrival_rows=%s", len(successfully_loaded_files), loaded_rows)

    successfully_empty_files = set()
    if empty_files:
        register_file_results(empty_files)
        successfully_empty_files = {item["source_file"] for item in empty_files}
        logger.info("event=zero_arrival_files_registered files=%s", len(successfully_empty_files))

    if parse_failures:
        register_file_results(parse_failures)
        logger.warning("event=parse_failures_registered files=%s", len(parse_failures))

    safe_to_archive = [
        path for path in fetched_files
        if Path(path).name in successfully_loaded_files or Path(path).name in successfully_empty_files
    ]
    if safe_to_archive:
        archive_files(safe_to_archive)
        logger.info("event=files_archived count=%s", len(safe_to_archive))

    logger.info(
        "event=pipeline_completed uploaded_files=%s loaded_files=%s zero_arrival_files=%s parse_failures=%s archived_files=%s arrival_rows=%s duration_seconds=%.2f",
        len(fetched_files), len(successfully_loaded_files), len(successfully_empty_files),
        len(parse_failures), len(safe_to_archive), loaded_rows, time.monotonic() - started,
    )


if __name__ == "__main__":
    main()
