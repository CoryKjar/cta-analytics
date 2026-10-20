"""Read-only CTA pipeline health check with deduplicated email alerts.

Environment:
  DATABASE_URL (required)
  SMTP_HOST, SMTP_PORT, SMTP_USERNAME, SMTP_PASSWORD
  ALERT_FROM_EMAIL, ALERT_TO_EMAIL
  COLLECTION_STALE_MINUTES=15
  MAX_ATTEMPTS=5
"""
import os
import smtplib
from email.message import EmailMessage
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

import psycopg
from dotenv import load_dotenv
from src.config.stations import STATIONS
from src.utils.logger import get_logger

load_dotenv()
logger = get_logger(__name__)

STALE_MINUTES = int(os.getenv("COLLECTION_STALE_MINUTES", "15"))
MAX_ATTEMPTS = int(os.getenv("SELF_HEAL_MAX_ATTEMPTS", "5"))
COLLECTION_RUN_STALE_MINUTES = int(os.getenv("COLLECTION_RUN_STALE_MINUTES", "15"))
COLLECTION_RUN_STALE_MINUTES = int(os.getenv("COLLECTION_RUN_STALE_MINUTES", "15"))


def connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(url, connect_timeout=15)


def collect_health(*, collection_window_hours=24):
    """Return current health metrics. Does not modify ingestion data."""
    with connect() as conn:
        latest = conn.execute("""
            SELECT MAX(collection_timestamp AT TIME ZONE 'America/Chicago')
            FROM ingestion_files
            WHERE status = 'processed'
        """).fetchone()[0]


        statuses = conn.execute("""
            SELECT status, COUNT(*)::bigint
            FROM ingestion_files
            GROUP BY status
        """).fetchall()
        status_counts = {status: count for status, count in statuses}

        retry_rows = conn.execute("""
            SELECT source_file, status, attempts, last_error, updated_at
            FROM ingestion_files
            WHERE status <> 'processed' AND attempts >= %s
            ORDER BY updated_at DESC
            LIMIT 100
        """, (MAX_ATTEMPTS,)).fetchall()

        nonprocessed = conn.execute("""
            SELECT source_file, status, attempts, last_error, updated_at
            FROM ingestion_files
            WHERE status <> 'processed'
            ORDER BY updated_at DESC
            LIMIT 100
        """).fetchall()

        file_issue_logs = conn.execute("""
            SELECT source_file, status, attempts, last_error, updated_at
            FROM ingestion_files
            WHERE (status <> 'processed' OR attempts > 0 OR last_error IS NOT NULL)
              AND (%s::integer IS NULL OR collection_timestamp AT TIME ZONE 'America/Chicago'
                   >= NOW() - (%s * INTERVAL '1 hour'))
            ORDER BY updated_at DESC
            LIMIT 100
        """, (collection_window_hours, collection_window_hours)).fetchall()

        mismatch_count = conn.execute("""
            SELECT COUNT(*)::bigint
            FROM (
                SELECT f.file_id
                FROM ingestion_files f
                LEFT JOIN arrivals a ON a.file_id = f.file_id
                WHERE f.status = 'processed'
                GROUP BY f.file_id, f.row_count
                HAVING COALESCE(f.row_count, 0) <> COUNT(a.*)
            ) mismatches
        """).fetchone()[0]

        recent_updates = conn.execute("""
            SELECT COUNT(*)::bigint
            FROM ingestion_files
            WHERE updated_at >= NOW() - INTERVAL '15 minutes'
        """).fetchone()[0]
        station_latest = dict(conn.execute("""
            SELECT station_id,MAX(collection_timestamp AT TIME ZONE 'America/Chicago')
            FROM ingestion_files WHERE status='processed' AND station_id IS NOT NULL
            GROUP BY station_id
        """).fetchall())

        has_collection_ledger = conn.execute(
            "SELECT to_regclass('public.collection_runs')"
        ).fetchone()[0] is not None
        stale_collection_runs = []
        recent_partial_collection_runs = []
        recent_collection_runs = []
        collection_attempt_logs = []
        if has_collection_ledger:
            stale_collection_runs = conn.execute("""
                SELECT r.run_id::text, r.started_at, r.expected_station_count,
                       COUNT(*) FILTER (WHERE a.status = 'uploaded')::bigint AS uploaded,
                       COUNT(*) FILTER (WHERE a.status = 'failed')::bigint AS failed,
                       COUNT(*) FILTER (WHERE a.status IN ('pending', 'in_progress'))::bigint AS unfinished
                FROM public.collection_runs AS r
                LEFT JOIN public.collection_attempts AS a USING (run_id)
                WHERE r.status = 'running'
                  AND r.started_at < NOW() - (%s * INTERVAL '1 minute')
                GROUP BY r.run_id
                ORDER BY r.started_at DESC
                LIMIT 10
            """, (COLLECTION_RUN_STALE_MINUTES,)).fetchall()
            recent_partial_collection_runs = conn.execute("""
                SELECT run_id::text, started_at, status, expected_station_count,
                      uploaded_station_count, failed_station_count, fatal_error_type
                FROM public.collection_runs
                WHERE status IN ('partial', 'failed')
                  AND completed_at >= NOW() - (%s * INTERVAL '1 minute')
                ORDER BY completed_at DESC
                LIMIT 10
            """, (COLLECTION_RUN_STALE_MINUTES,)).fetchall()
            recent_partial_collection_runs = conn.execute("""
                SELECT run_id::text, started_at, status, expected_station_count,
                       uploaded_station_count, failed_station_count, fatal_error_type
                FROM public.collection_runs
                WHERE status IN ('partial', 'failed')
                  AND completed_at >= NOW() - INTERVAL '24 hours'
                ORDER BY completed_at DESC
                LIMIT 20
            """).fetchall()
            recent_collection_runs = conn.execute("""
                SELECT r.run_id::text, r.started_at, r.completed_at, r.status,
                       r.expected_station_count, r.uploaded_station_count,
                       r.failed_station_count, r.fatal_error_type,
                       COUNT(*) FILTER (WHERE a.status = 'pending')::bigint AS pending,
                       COUNT(*) FILTER (WHERE a.status = 'in_progress')::bigint AS in_progress
                FROM public.collection_runs AS r
                LEFT JOIN public.collection_attempts AS a USING (run_id)
                WHERE (%s::integer IS NULL OR r.started_at >= NOW() - (%s * INTERVAL '1 hour'))
                GROUP BY r.run_id
                ORDER BY r.started_at DESC
                LIMIT 20
            """, (collection_window_hours, collection_window_hours)).fetchall()
            collection_attempt_logs = conn.execute("""
                SELECT r.run_id::text, r.started_at, a.station_name, a.station_id,
                       a.station_index, a.status, a.stage, a.request_attempts,
                       a.http_status, a.cta_error_code, a.storage_error_code,
                       a.storage_http_status, a.error_type, a.source_file,
                       a.object_key, a.duration_seconds
                FROM public.collection_attempts AS a
                JOIN public.collection_runs AS r USING (run_id)
                                WHERE (%s::integer IS NULL OR r.started_at >= NOW() - (%s * INTERVAL '1 hour'))
                  AND (a.status <> 'uploaded' OR a.request_attempts > 1)
                ORDER BY r.started_at DESC, a.station_index
                LIMIT 100
                        """, (collection_window_hours, collection_window_hours)).fetchall()

    now = datetime.now(timezone.utc)
    age_minutes = None
    if latest is not None:
        # The SQL expression returns a UTC-aware value when the source column is
        # a naive Chicago timestamp, but can return a naive Chicago wall time if
        # the source column is timestamptz. Normalize both cases to UTC before
        # calculating age or exposing the timestamp to the dashboard.
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=ZoneInfo("America/Chicago"))
        latest = latest.astimezone(timezone.utc)
        age_minutes = max(0.0, (now - latest).total_seconds() / 60.0)

    issues = {}
    stale_stations = []
    for name, station_id in STATIONS.items():
        collected = station_latest.get(station_id)
        if collected is not None and collected.tzinfo is None:
            collected = collected.replace(tzinfo=ZoneInfo("America/Chicago"))
        if collected is None or (now-collected.astimezone(timezone.utc)).total_seconds()/60 > STALE_MINUTES:
            stale_stations.append(name)
    if stale_stations:
        issues["station_collection_stale"] = (
            f"{len(stale_stations)} configured station(s) have no fresh processed snapshot: "
            + ", ".join(stale_stations[:8])
        )
    if latest is None:
        issues["collection_stale"] = "No processed snapshots exist."
    elif age_minutes > STALE_MINUTES:
        issues["collection_stale"] = (
            f"Latest processed snapshot is {age_minutes:.1f} minutes old "
            f"(threshold: {STALE_MINUTES} minutes)."
        )
    if status_counts.get("failed", 0):
        issues["failed_files"] = f"{status_counts['failed']} file(s) have status failed."
    if status_counts.get("processing", 0):
        issues["processing_files"] = (
            f"{status_counts['processing']} file(s) have status processing."
        )

    # Keep known operational states on their existing alert paths, but flag any
    # new/unrecognized status so it cannot silently appear only in the dashboard.
    known_statuses = {"processed", "failed", "processing"}
    unexpected_statuses = {
        status: count
        for status, count in status_counts.items()
        if status not in known_statuses
    }
    if unexpected_statuses:
        status_summary = ", ".join(
            f"{status!r}: {count}"
            for status, count in sorted(
                unexpected_statuses.items(), key=lambda item: str(item[0])
            )
        )
        issues["unexpected_statuses"] = (
            f"Unrecognized ingestion status(es) detected: {status_summary}."
        )

    if retry_rows:
        issues["retry_cap"] = (
            f"{len(retry_rows)} non-processed file(s) are at or above "
            f"{MAX_ATTEMPTS} attempts (query capped at 100 rows)."
        )
    if mismatch_count:
        issues["row_count_mismatch"] = (
            f"{mismatch_count} processed manifest row-count mismatch(es)."
        )
    if stale_collection_runs:
        run_id, started_at, expected, uploaded, failed, unfinished = stale_collection_runs[0]
        issues["collection_run_incomplete"] = (
            f"Run {run_id[:8]} started at {started_at.isoformat()} is still running after "
            f"{COLLECTION_RUN_STALE_MINUTES} minutes: {uploaded}/{expected} uploaded, "
            f"{failed} failed, {unfinished} pending/in progress."
        )
    if recent_partial_collection_runs:
        run_id, started_at, status, expected, uploaded, failed, fatal_error_type = recent_partial_collection_runs[0]
        fatal_detail = f", fatal error {fatal_error_type}" if fatal_error_type else ""
        issues["partial_collection_run"] = (
            f"Run {run_id[:8]} at {started_at.isoformat()} ended {status}: "
            f"{uploaded}/{expected} uploaded, {failed} failed{fatal_detail}."
        )
    return {
        "checked_at": now,
        "latest_snapshot": latest,
        "age_minutes": age_minutes,
        "status_counts": status_counts,
        "retry_rows": retry_rows,
        "nonprocessed_rows": nonprocessed,
        "file_issue_logs": file_issue_logs,
        "mismatch_count": mismatch_count,
        "stale_collection_runs": stale_collection_runs,
        "recent_partial_collection_runs": recent_partial_collection_runs,
        "has_collection_ledger": has_collection_ledger,
        "recent_collection_runs": recent_collection_runs,
        "collection_attempt_logs": collection_attempt_logs,
        "recent_updates": recent_updates,
        "issues": issues,
        "stale_stations": stale_stations,
    }


def send_email(subject, body):
    """Send an alert if SMTP settings are configured; otherwise raise."""
    required = ["SMTP_HOST", "ALERT_FROM_EMAIL", "ALERT_TO_EMAIL"]
    missing = [key for key in required if not os.getenv(key)]
    if missing:
        raise RuntimeError(f"Email settings missing: {', '.join(missing)}")

    msg = EmailMessage()
    msg["Subject"] = subject
    msg["From"] = os.environ["ALERT_FROM_EMAIL"]
    msg["To"] = os.environ["ALERT_TO_EMAIL"]
    msg.set_content(body)

    host = os.environ["SMTP_HOST"]
    port = int(os.getenv("SMTP_PORT", "587"))
    username = os.getenv("SMTP_USERNAME")
    password = os.getenv("SMTP_PASSWORD")

    with smtplib.SMTP(host, port, timeout=20) as server:
        server.ehlo()
        server.starttls()
        server.ehlo()
        if username:
            server.login(username, password or "")
        server.send_message(msg)


def evaluate_and_alert():
    """Alert only on new/changed issue sets; send a recovery email when healthy again."""
    logger.info("event=health_check_started")
    try:
        return _evaluate_and_alert()
    except Exception as exc:
        logger.error("event=health_check_failed error_type=%s", type(exc).__name__)
        raise


def _evaluate_and_alert():
    health = collect_health()
    for key, detail in sorted(health["issues"].items()):
        logger.error("event=health_issue issue=%s detail=%s", key, detail)
    issue_items = sorted(health["issues"].items())
    # Deduplicate by issue type, not changing counts/ages, to avoid alert spam.
    current_signature = "\n".join(k for k, _ in issue_items)
    issue_details = "\n".join(f"{key}: {detail}" for key, detail in issue_items)
    state_key = "pipeline_health"

    # State is stored in a dedicated monitoring table, not in ingestion_files.
    with connect() as conn:
        row = conn.execute("""
            SELECT issue_signature
            FROM monitoring_alert_state
            WHERE state_key = %s
        """, (state_key,)).fetchone()
        previous_signature = row[0] if row else None

        if current_signature != (previous_signature or ""):
            if current_signature:
                subject = "[CTA ALERT] Pipeline health issue"
                body = (
                    f"CTA pipeline health check at {health['checked_at'].isoformat()}\n\n"
                    + issue_details
                    + f"\n\nLatest processed snapshot: {health['latest_snapshot']}\n"
                    + f"Processed files updated in last 15 minutes: {health['recent_updates']}\n"
                )
            elif previous_signature:
                subject = "[CTA RECOVERED] Pipeline health is clear"
                body = (
                    f"CTA pipeline health returned to normal at "
                    f"{health['checked_at'].isoformat()}.\n"
                    "No monitored issues are currently detected."
                )
            else:
                subject = body = None

            # Persist state only after email succeeds. First clean run creates baseline silently.
            if subject:
                send_email(subject, body)
                logger.info("event=health_email_sent kind=%s", "alert" if current_signature else "recovery")

            conn.execute("""
                INSERT INTO monitoring_alert_state
                    (state_key, issue_signature, checked_at, last_email_at)
                VALUES (%s, %s, NOW(), CASE WHEN %s THEN NOW() ELSE NULL END)
                ON CONFLICT (state_key) DO UPDATE SET
                    issue_signature = EXCLUDED.issue_signature,
                    checked_at = NOW(),
                    last_email_at = CASE
                        WHEN %s THEN NOW()
                        ELSE monitoring_alert_state.last_email_at
                    END
            """, (state_key, current_signature, bool(subject), bool(subject)))
        else:
            conn.execute("""
                UPDATE monitoring_alert_state
                SET checked_at = NOW()
                WHERE state_key = %s
            """, (state_key,))

    log = logger.error if health["issues"] else logger.info
    log(
        "event=health_check_completed issues=%s latest_snapshot=%s recent_updates=%s stale_stations=%s mismatch_count=%s",
        len(health["issues"]), health["latest_snapshot"], health["recent_updates"],
        len(health["stale_stations"]), health["mismatch_count"],
    )
    return health


if __name__ == "__main__":
    # A DB connection failure or email failure exits non-zero so Railway logs show failure.
    try:
        evaluate_and_alert()
    except Exception:
        raise SystemExit(1) from None
