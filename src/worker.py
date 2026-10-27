"""Opt-in persistent collector: one collection per UTC-aligned five-minute interval.

    python -m src.worker

Deploy only on a service WITHOUT a cron schedule. The cron entry point remains
``python -m src.pipeline``. Both share a database lock and per-interval check,
so an accidental second scheduler skips rather than collecting twice.
"""
import os
import signal
import threading
import time
from datetime import datetime, timedelta, timezone

import psycopg
from dotenv import load_dotenv

from src.monitoring.collection_gaps import find_collection_gaps, group_episodes
from src.pipeline import _run_pipeline
from src.storage.collection_ledger import COLLECTION_INTERVAL_SECONDS, CollectionSkipped
from src.utils.logger import get_logger

logger = get_logger(__name__)

# Starting later than this into an interval risks overrunning the next one.
LATE_START_SECONDS = 120
WATCHDOG_POLL_SECONDS = 5


def next_interval(now_epoch, last_interval):
    """Return (interval index, due epoch) for the next collection."""
    current = int(now_epoch // COLLECTION_INTERVAL_SECONDS)
    if current != last_interval and now_epoch - current * COLLECTION_INTERVAL_SECONDS <= LATE_START_SECONDS:
        return current, now_epoch
    return current + 1, (current + 1) * COLLECTION_INTERVAL_SECONDS


def log_recent_gaps(hours):
    """Report persisted gaps on startup; never prevents the worker from starting."""
    try:
        end = datetime.now(timezone.utc)
        with psycopg.connect(
            os.environ["DATABASE_URL"], connect_timeout=10,
            options="-c default_transaction_read_only=on -c statement_timeout=60000",
        ) as conn:
            episodes = group_episodes(find_collection_gaps(conn, start=end - timedelta(hours=hours), end=end))
    except Exception as exc:
        logger.error("event=worker_gap_check_failed error_type=%s", type(exc).__name__)
        return
    for episode in episodes:
        logger.warning(
            "event=collection_gap_detected first_interval=%s intervals=%s state=%s cause=%s awaiting_recovery=%s",
            episode["first_interval"].isoformat(), episode["intervals"], episode["state"],
            episode["cause"], episode["awaiting_recovery"],
        )
    logger.info("event=worker_gap_check_completed hours=%s episodes=%s", hours, len(episodes))


def run_cycle(interval):
    started = time.monotonic()
    try:
        _run_pipeline()
        outcome, detail = "completed", None
    except CollectionSkipped as skipped:
        outcome, detail = "skipped", skipped.reason
    except Exception as exc:
        outcome, detail = "failed", type(exc).__name__
    log = logger.error if outcome == "failed" else logger.info
    log(
        "event=worker_cycle_finished interval_start=%s outcome=%s detail=%s duration_seconds=%.2f",
        datetime.fromtimestamp(interval * COLLECTION_INTERVAL_SECONDS, timezone.utc).isoformat(),
        outcome, detail, time.monotonic() - started,
    )
    return outcome


def start_watchdog(cycle_started, timeout_seconds, exit_process, finished):
    """Exit non-zero if one cycle runs too long; a hung process is otherwise never restarted."""
    poll_seconds = min(WATCHDOG_POLL_SECONDS, timeout_seconds / 2)

    def watch():
        while not finished.wait(poll_seconds):
            started = cycle_started[0]
            if started is not None and time.monotonic() - started > timeout_seconds:
                logger.error("event=worker_cycle_timeout timeout_seconds=%s action=exit", timeout_seconds)
                exit_process(1)
                return

    thread = threading.Thread(target=watch, name="worker-watchdog", daemon=True)
    thread.start()
    return thread


def run_worker(
    stop=None, *, clock=time.time, cycle=run_cycle, startup_gap_hours=None,
    cycle_timeout_seconds=None, exit_process=os._exit,
):
    stop = stop or threading.Event()
    load_dotenv()
    if startup_gap_hours is None:
        startup_gap_hours = int(os.getenv("WORKER_STARTUP_GAP_HOURS", "6"))
    if cycle_timeout_seconds is None:
        cycle_timeout_seconds = float(os.getenv("WORKER_CYCLE_TIMEOUT_SECONDS", "600"))
    cycle_started = [None]
    finished = threading.Event()

    def request_stop(signum, frame):
        stop.set()

    previous = {}
    if threading.current_thread() is threading.main_thread():
        for signum in (signal.SIGTERM, signal.SIGINT):
            previous[signum] = signal.signal(signum, request_stop)
    logger.info("event=worker_started pid=%s interval_seconds=%s", os.getpid(), COLLECTION_INTERVAL_SECONDS)
    try:
        if startup_gap_hours > 0:
            log_recent_gaps(startup_gap_hours)
        if cycle_timeout_seconds > 0:
            start_watchdog(cycle_started, cycle_timeout_seconds, exit_process, finished)
        last_interval = None
        while not stop.is_set():
            interval, due = next_interval(clock(), last_interval)
            # Event.wait uses a monotonic clock; recheck wall time in case it jumped backwards.
            if stop.wait(max(0.0, due - clock())):
                break
            if clock() < due:
                continue
            last_interval = interval
            cycle_started[0] = time.monotonic()
            try:
                cycle(interval)
            finally:
                cycle_started[0] = None
    finally:
        finished.set()
        for signum, handler in previous.items():
            signal.signal(signum, handler)
        logger.info("event=worker_stopped pid=%s shutdown_requested=%s", os.getpid(), stop.is_set())


if __name__ == "__main__":
    try:
        run_worker()
    except Exception as exc:
        logger.error("event=worker_failed error_type=%s", type(exc).__name__)
        raise SystemExit(1) from None
