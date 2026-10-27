"""Read-only detection of missed or incomplete five-minute collection intervals.

Intervals are UTC-aligned (matching the Railway cron ``*/5``) and judged from the
persisted collection ledger, so results survive restarts and do not depend on
any process's memory. Only fully elapsed intervals are evaluated.

    python -m src.monitoring.collection_gaps --hours 24
"""
import argparse
import os
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv

from src.storage.collection_ledger import COLLECTION_INTERVAL_SECONDS

INTERVAL = timedelta(seconds=COLLECTION_INTERVAL_SECONDS)
MAX_REPORT_HOURS = 24 * 31
# Lower rank wins when several runs started in one interval.
RUN_RANK = {"completed": 0, "running": 1, "partial": 2, "failed": 3}

RECOVERABILITY = {
    "awaiting_recovery": "XML archived or held in pending_uploads; hourly recovery loads it",
    "partial": "stations not archived cannot be recovered",
    "failed": "no snapshot archived; not recoverable",
    "missed": "no snapshot collected; not recoverable (CTA API has no history)",
    "unrecorded": "data present; ledger write was unavailable",
    "incomplete": "completion never recorded; reconcile the run before judging",
}


def interval_start(moment):
    seconds = int(moment.timestamp()) // COLLECTION_INTERVAL_SECONDS * COLLECTION_INTERVAL_SECONDS
    return datetime.fromtimestamp(seconds, timezone.utc)


def fetch_gap_inputs(conn, start, end):
    """Return ledger start, runs near the window, and processed-file interval starts."""
    runs = conn.execute("""
        WITH gap_runs AS (
            SELECT run_id, started_at, completed_at, status, expected_station_count
            FROM public.collection_runs
            WHERE started_at >= %s - INTERVAL '1 hour' AND started_at < %s
        )
        SELECT g.started_at, g.completed_at, g.status, g.expected_station_count,
               COUNT(a.*) FILTER (WHERE a.status = 'uploaded')::integer,
               COUNT(a.*) FILTER (
                   WHERE (a.status = 'uploaded' OR a.stage = 'upload_deferred')
                     AND f.status IS DISTINCT FROM 'processed'
               )::integer,
               COUNT(a.*) FILTER (WHERE a.stage = 'upload_deferred')::integer,
               (SELECT MIN(started_at) FROM public.collection_runs)
        FROM gap_runs AS g
        LEFT JOIN public.collection_attempts AS a USING (run_id)
        LEFT JOIN ingestion_files AS f ON f.source_file = a.source_file
        GROUP BY g.run_id, g.started_at, g.completed_at, g.status, g.expected_station_count
        ORDER BY g.started_at
    """, (start, end)).fetchall()
    ledger_start = runs[0][7] if runs else conn.execute(
        "SELECT MIN(started_at) FROM public.collection_runs"
    ).fetchone()[0]
    # Source timestamps are naive Chicago wall times; convert the UTC bounds to match.
    file_intervals = {
        row[0] for row in conn.execute("""
            SELECT DISTINCT to_timestamp(
                floor(extract(epoch FROM collection_timestamp AT TIME ZONE 'America/Chicago') / %s) * %s
            )
            FROM ingestion_files
            WHERE status = 'processed'
              AND collection_timestamp >= (%s::timestamptz AT TIME ZONE 'America/Chicago')
              AND collection_timestamp < (%s::timestamptz AT TIME ZONE 'America/Chicago')
        """, (COLLECTION_INTERVAL_SECONDS, COLLECTION_INTERVAL_SECONDS, start, end)).fetchall()
    }
    return ledger_start, [row[:7] for row in runs], file_intervals


def classify_intervals(runs, file_intervals, *, start, end, ledger_start, now, stale_after):
    """Classify each fully elapsed interval in [start, end); return non-completed ones.

    ``runs`` rows: (started_at, completed_at, status, expected, uploaded, awaiting_recovery, deferred).
    Deferred stations were held in pending_uploads after a bucket failure, so they are recoverable.
    """
    if ledger_start is None:
        return []
    first = max(interval_start(start), interval_start(ledger_start))
    last = min(interval_start(end), interval_start(now))
    by_interval = {}
    for run in runs:
        by_interval.setdefault(interval_start(run[0]), []).append(run)

    gaps = []
    current = first
    while current < last:
        candidates = by_interval.get(current, [])
        if candidates:
            started, _completed, status, expected, uploaded, awaiting, deferred = min(
                candidates, key=lambda run: RUN_RANK.get(run[2], 9),
            )
            if status == "running" and now - started > stale_after:
                state = "incomplete"
            elif status != "running" and uploaded + deferred >= expected:
                state = "awaiting_recovery" if awaiting else "completed"
            else:
                state = status
            if state not in ("completed", "running"):
                gaps.append({
                    "interval_start": current, "state": state, "cause": None,
                    "expected": expected, "uploaded": uploaded, "awaiting_recovery": awaiting,
                    "deferred": deferred,
                })
        else:
            overlapping = any(
                run[0] < current and (
                    run[1] > current if run[1] is not None
                    else run[2] == "running" and current - run[0] <= stale_after
                )
                for run in runs
            )
            if overlapping:
                state, cause = "missed", "previous_run_still_active"
            elif current in file_intervals:
                state, cause = "unrecorded", "ledger_unavailable"
            else:
                state, cause = "missed", "not_started"
            gaps.append({
                "interval_start": current, "state": state, "cause": cause,
                "expected": None, "uploaded": 0, "awaiting_recovery": 0, "deferred": 0,
            })
        current += INTERVAL
    return gaps


def find_collection_gaps(conn, *, start, end, now=None, stale_after=timedelta(minutes=15)):
    now = now or datetime.now(timezone.utc)
    ledger_start, runs, file_intervals = fetch_gap_inputs(conn, start, end)
    return classify_intervals(
        runs, file_intervals, start=start, end=end,
        ledger_start=ledger_start, now=now, stale_after=stale_after,
    )


def group_episodes(gaps):
    """Merge consecutive intervals with the same state and cause."""
    episodes = []
    for gap in gaps:
        last = episodes[-1] if episodes else None
        if (last and last["state"] == gap["state"] and last["cause"] == gap["cause"]
                and gap["interval_start"] - last["last_interval"] == INTERVAL):
            last["last_interval"] = gap["interval_start"]
            last["intervals"] += 1
            last["awaiting_recovery"] += gap["awaiting_recovery"]
        else:
            episodes.append({
                "state": gap["state"], "cause": gap["cause"],
                "first_interval": gap["interval_start"], "last_interval": gap["interval_start"],
                "intervals": 1, "awaiting_recovery": gap["awaiting_recovery"],
            })
    return episodes


def main(argv=None):
    parser = argparse.ArgumentParser(description="Read-only report of missed collection intervals.")
    parser.add_argument("--hours", type=int, default=24)
    args = parser.parse_args(argv)
    hours = max(1, min(args.hours, MAX_REPORT_HOURS))
    load_dotenv()
    url = os.getenv("DATABASE_URL")
    if not url:
        raise SystemExit("DATABASE_URL is not configured")
    end = datetime.now(timezone.utc)
    chicago = ZoneInfo("America/Chicago")
    with psycopg.connect(
        url, connect_timeout=15,
        options="-c default_transaction_read_only=on -c statement_timeout=120000",
    ) as conn:
        episodes = group_episodes(find_collection_gaps(conn, start=end - timedelta(hours=hours), end=end))
    print(f"Collection intervals not fully collected in the last {hours}h: {len(episodes)} episode(s)")
    for episode in episodes:
        first = episode["first_interval"]
        last = episode["last_interval"] + INTERVAL
        print(
            f"{first:%Y-%m-%d %H:%M}Z-{last:%H:%M}Z "
            f"({first.astimezone(chicago):%Y-%m-%d %H:%M %Z}) "
            f"intervals={episode['intervals']} state={episode['state']} "
            f"cause={episode['cause'] or '-'}: {RECOVERABILITY[episode['state']]}"
        )


if __name__ == "__main__":
    main()
