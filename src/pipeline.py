from datetime import datetime
from pathlib import Path
import os
import signal
import time
import pandas as pd
import requests

from src.ingestion.fetch_arrivals import fetch_arrivals
from src.transform.parse_arrivals import parse_arrivals
from src.storage.collection_ledger import CollectionSkipped
from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results
from src.utils.cleanup_staging import cleanup_staged_files
from src.utils.logger import get_logger
from src.config.stations import STATIONS

logger = get_logger(__name__)


def _timestamp_from_path(path):
    parts = Path(path).stem.split("_")
    if len(parts) < 3:
        raise ValueError(f"Unexpected XML filename: {Path(path).name}")
    return datetime.strptime(f"{parts[0]}_{parts[1]}", "%Y%m%d_%H%M%S")


def _register_failures(records):
    try:
        register_file_results(records)
    except Exception as exc:
        logger.error(
            "event=file_failure_registration_failed files=%s error_type=%s",
            len(records), type(exc).__name__,
        )


def _send_heartbeat():
    """Ping an external dead-man's switch; its silence alerts even if Railway stops all jobs."""
    url = os.getenv("COLLECTION_HEARTBEAT_URL", "").strip()
    if not url:
        return
    if not url.startswith("https://"):
        logger.error("event=heartbeat_skipped reason=https_required")
        return
    try:
        requests.get(url, timeout=(5, 10)).raise_for_status()
        logger.info("event=heartbeat_sent")
    except Exception as exc:
        # The URL embeds a check token, so never log it or the exception text.
        logger.error("event=heartbeat_failed error_type=%s", type(exc).__name__)


def main():
    previous_handler = signal.getsignal(signal.SIGTERM)
    shutdown_requested_at = None
    started = time.monotonic()
    completed = False

    def request_shutdown(signum, frame):
        nonlocal shutdown_requested_at
        if shutdown_requested_at is None:
            shutdown_requested_at = time.monotonic()

    signal.signal(signal.SIGTERM, request_shutdown)
    try:
        _run_pipeline()
        completed = True
    finally:
        signal.signal(signal.SIGTERM, previous_handler)
        if shutdown_requested_at is not None:
            logger.warning(
                "event=pipeline_shutdown_requested signal=SIGTERM action=finish_current_run pid=%s duration_seconds=%.2f",
                os.getpid(), shutdown_requested_at - started,
            )
            if completed:
                logger.info(
                    "event=pipeline_shutdown_completed pid=%s duration_seconds=%.2f",
                    os.getpid(), time.monotonic() - started,
                )
            else:
                logger.error(
                    "event=pipeline_shutdown_incomplete pid=%s duration_seconds=%.2f",
                    os.getpid(), time.monotonic() - started,
                )


def _run_pipeline():
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
            logger.error("event=self_heal_failed error_type=%s", type(exc).__name__)

    fetched_files = fetch_arrivals()
    if not fetched_files:
            logger.error("event=pipeline_no_files_uploaded duration_seconds=%.2f", time.monotonic() - started)
            raise RuntimeError("No station snapshots were successfully collected and uploaded.")

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
                "last_error": f"parse: {type(exc).__name__}",
            })
            logger.error("event=file_parse_failed file=%r error_type=%s", path.name, type(exc).__name__)

    if parse_failures:
        _register_failures(parse_failures)
        logger.warning("event=parse_failures_detected files=%s", len(parse_failures))

    successfully_loaded_files = set()
    loaded_rows = 0
    if parsed_frames:
        all_rows = pd.concat(parsed_frames, ignore_index=True)
        loaded_rows = len(all_rows)
        try:
            load_arrivals_data(all_rows)
        except Exception as exc:
            failures = []
            for name in all_rows["source_file"].unique():
                failures.append({
                    "source_file": name,
                    "collection_timestamp": _timestamp_from_path(name),
                    "station_id": None, "status": "failed", "row_count": 0,
                    "last_error": f"load: {type(exc).__name__}",
                })
                logger.error("event=file_load_failed file=%r error_type=%s", name, type(exc).__name__)
            _register_failures(failures)
            raise
        successfully_loaded_files = set(all_rows["source_file"].unique())
        logger.info("event=database_load_succeeded files=%s arrival_rows=%s", len(successfully_loaded_files), loaded_rows)

    successfully_empty_files = set()
    if empty_files:
        try:
            register_file_results(empty_files)
        except Exception as exc:
            failures = [
                {**item, "status": "failed", "last_error": f"empty-file registration: {type(exc).__name__}"}
                for item in empty_files
            ]
            for item in failures:
                logger.error("event=empty_file_registration_failed file=%r error_type=%s", item["source_file"], type(exc).__name__)
            _register_failures(failures)
            raise
        successfully_empty_files = {item["source_file"] for item in empty_files}
        logger.info("event=zero_arrival_files_registered files=%s", len(successfully_empty_files))

    safe_to_archive = [
        path for path in fetched_files
        if Path(path).name in successfully_loaded_files or Path(path).name in successfully_empty_files
    ]
    if safe_to_archive:
        cleanup_staged_files(safe_to_archive)
        logger.info("event=files_archived count=%s", len(safe_to_archive))

    incomplete = bool(parse_failures) or len(fetched_files) != len(STATIONS)
    log_summary = logger.error if incomplete else logger.info
    log_summary(
        "event=%s uploaded_files=%s expected_files=%s loaded_files=%s zero_arrival_files=%s parse_failures=%s archived_files=%s arrival_rows=%s duration_seconds=%.2f",
        "pipeline_incomplete" if incomplete else "pipeline_completed",
        len(fetched_files), len(STATIONS), len(successfully_loaded_files), len(successfully_empty_files),
        len(parse_failures), len(safe_to_archive), loaded_rows, time.monotonic() - started,
    )

    if not incomplete:
        _send_heartbeat()

    if parse_failures:
        raise RuntimeError(f"Collection completed with {len(parse_failures)} failed source files.")
    if len(fetched_files) != len(STATIONS):
        raise RuntimeError(f"Collection uploaded {len(fetched_files)} of {len(STATIONS)} configured station snapshots.")


if __name__ == "__main__":
    try:
        main()
    except CollectionSkipped as skipped:
        logger.warning("event=pipeline_skipped reason=%s", skipped.reason)
    except Exception as exc:
        logger.error("event=pipeline_failed error_type=%s", type(exc).__name__)
        raise SystemExit(1) from None
