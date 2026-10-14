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

import psycopg
from dotenv import load_dotenv

load_dotenv()

STALE_MINUTES = int(os.getenv("COLLECTION_STALE_MINUTES", "15"))
MAX_ATTEMPTS = int(os.getenv("SELF_HEAL_MAX_ATTEMPTS", "5"))


def connect():
    url = os.getenv("DATABASE_URL")
    if not url:
        raise RuntimeError("DATABASE_URL is not configured")
    return psycopg.connect(url, connect_timeout=15)


def collect_health():
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

    now = datetime.now(timezone.utc)
    age_minutes = None
    if latest is not None:
        if latest.tzinfo is None:
            latest = latest.replace(tzinfo=timezone.utc)
        age_minutes = max(0.0, (now - latest).total_seconds() / 60.0)

    issues = {}
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
    if retry_rows:
        issues["retry_cap"] = (
            f"{len(retry_rows)} non-processed file(s) are at or above "
            f"{MAX_ATTEMPTS} attempts (query capped at 100 rows)."
        )
    if mismatch_count:
        issues["row_count_mismatch"] = (
            f"{mismatch_count} processed manifest row-count mismatch(es)."
        )

    return {
        "checked_at": now,
        "latest_snapshot": latest,
        "age_minutes": age_minutes,
        "status_counts": status_counts,
        "retry_rows": retry_rows,
        "nonprocessed_rows": nonprocessed,
        "mismatch_count": mismatch_count,
        "recent_updates": recent_updates,
        "issues": issues,
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
    health = collect_health()
    issue_items = sorted(health["issues"].items())
    # Deduplicate by issue type, not changing counts/ages, to avoid alert spam.
    current_signature = "\n".join(k for k, _ in issue_items)
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
                    + current_signature
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

    print(
        f"Health check complete: issues={len(health['issues'])}, "
        f"latest_snapshot={health['latest_snapshot']}, "
        f"recent_updates={health['recent_updates']}",
        flush=True,
    )
    for key, detail in health["issues"].items():
        print(f"HEALTH ISSUE [{key}]: {detail}", flush=True)
    return health


if __name__ == "__main__":
    # A DB connection failure or email failure exits non-zero so Railway logs show failure.
    evaluate_and_alert()
