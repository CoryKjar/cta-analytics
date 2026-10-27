"""Best-effort durable records of collection runs and per-station outcomes."""
import os
from uuid import uuid4

import psycopg

# Shared by every collector process (cron, worker or manual) so only one collects at a time.
COLLECTION_LOCK_KEY = 4_727_301_605
COLLECTION_INTERVAL_SECONDS = 300
# Collector write connections fail a stuck lock or query instead of blocking the run forever.
DATABASE_TIMEOUT_OPTIONS = "-c lock_timeout=30s -c statement_timeout=120s"


class CollectionSkipped(Exception):
    """Another collector holds the lock, or this five-minute interval was already collected."""

    def __init__(self, reason):
        super().__init__(reason)
        self.reason = reason


def collection_retry_details_available(connection):
    return connection.execute("""
        SELECT COUNT(*)
        FROM information_schema.columns
        WHERE table_schema = 'public' AND table_name = 'collection_attempts'
          AND column_name IN ('last_retry_http_status', 'last_retry_error_type')
    """).fetchone()[0] == 2


class CollectionAttemptLedger:
    def __init__(self, stations, logger):
        self.run_id = uuid4()
        self.expected_station_count = len(stations)
        self.logger = logger
        self.connection = None
        self.enabled = False
        self.retry_details_enabled = False

        try:
            database_url = os.getenv("DATABASE_URL")
            if not database_url:
                raise ValueError("DATABASE_URL is not configured")
            self.connection = psycopg.connect(
                database_url, connect_timeout=10, options=DATABASE_TIMEOUT_OPTIONS,
            )
            # Session lock: released when this connection closes, including process death.
            if not self.connection.execute(
                "SELECT pg_try_advisory_lock(%s)", (COLLECTION_LOCK_KEY,),
            ).fetchone()[0]:
                raise CollectionSkipped("collection_in_progress")
            # Failed or partial runs do not block a retry in the same interval.
            if self.connection.execute("""
                SELECT EXISTS (
                    SELECT 1 FROM public.collection_runs
                    WHERE status IN ('running', 'completed')
                      AND started_at >= to_timestamp(
                          floor(extract(epoch FROM NOW()) / %s) * %s
                      )
                )
            """, (COLLECTION_INTERVAL_SECONDS, COLLECTION_INTERVAL_SECONDS)).fetchone()[0]:
                raise CollectionSkipped("interval_already_collected")
            with self.connection.cursor() as cursor:
                cursor.execute("""
                    INSERT INTO public.collection_runs
                        (run_id, status, expected_station_count, deployment_commit)
                    VALUES (%s, 'running', %s, %s)
                """, (
                    self.run_id, self.expected_station_count,
                    os.getenv("RAILWAY_GIT_COMMIT_SHA"),
                ))
                cursor.executemany("""
                    INSERT INTO public.collection_attempts
                        (run_id, station_id, station_name, station_index, status, stage)
                    VALUES (%s, %s, %s, %s, 'pending', 'not_started')
                """, [
                    (self.run_id, station_id, name, index)
                    for index, (name, station_id) in enumerate(stations.items(), start=1)
                ])
            self.retry_details_enabled = collection_retry_details_available(self.connection)
            self.connection.commit()
            self.enabled = True
            if not self.retry_details_enabled:
                self.logger.warning(
                    "event=collection_retry_context_unavailable run_id=%s reason=migration_required",
                    self.run_id,
                )
            self.logger.info(
                "event=collection_run_started run_id=%s expected_stations=%s deployment_commit=%s",
                self.run_id, self.expected_station_count,
                os.getenv("RAILWAY_GIT_COMMIT_SHA", "unknown"),
            )
        except CollectionSkipped as skipped:
            self._close_connection()
            self.logger.warning(
                "event=collection_skipped run_id=%s reason=%s", self.run_id, skipped.reason,
            )
            raise
        except Exception as exc:
            self._disable("start", exc)

    def _close_connection(self):
        if self.connection is not None:
            try:
                self.connection.rollback()
                self.connection.close()
            except Exception:
                pass
        self.connection = None

    def _disable(self, stage, error):
        self._close_connection()
        if self.enabled or stage == "start":
            self.logger.error(
                "event=collection_ledger_unavailable run_id=%s stage=%s error_type=%s",
                self.run_id, stage, type(error).__name__,
            )
        self.enabled = False

    def _execute(self, stage, statement, parameters):
        if not self.enabled:
            return False
        try:
            with self.connection.cursor() as cursor:
                cursor.execute(statement, parameters)
            self.connection.commit()
            return True
        except Exception as exc:
            self._disable(stage, exc)
            return False

    def begin_station(self, station_id, *, started_at):
        self._execute("station_start", """
            UPDATE public.collection_attempts
            SET status='in_progress', stage='cta_request', request_attempts=1,
                started_at=%s
            WHERE run_id=%s AND station_id=%s AND status='pending'
        """, (started_at, self.run_id, station_id))

    def record_request_attempt(self, station_id, attempt):
        self._execute("request_retry", """
            UPDATE public.collection_attempts
            SET request_attempts=%s
            WHERE run_id=%s AND station_id=%s AND status='in_progress'
        """, (attempt, self.run_id, station_id))

    def record_retry_failure(self, station_id, *, http_status, error_type):
        if not self.retry_details_enabled:
            return
        self._execute("request_retry_failure", """
            UPDATE public.collection_attempts
            SET last_retry_http_status=%s, last_retry_error_type=%s
            WHERE run_id=%s AND station_id=%s AND status='in_progress'
        """, (http_status, error_type, self.run_id, station_id))

    def set_stage(self, station_id, stage, *, http_status=None, cta_error_code=None):
        self._execute("station_stage", """
            UPDATE public.collection_attempts
            SET stage=%s,
                http_status=COALESCE(%s, http_status),
                cta_error_code=COALESCE(%s, cta_error_code)
            WHERE run_id=%s AND station_id=%s AND status='in_progress'
        """, (stage, http_status, cta_error_code, self.run_id, station_id))

    def station_uploaded(
        self, station_id, *, attempt_count, http_status, cta_error_code,
        source_file, object_key, duration_seconds,
    ):
        self._execute("station_uploaded", """
            UPDATE public.collection_attempts
            SET status='uploaded', stage='complete', request_attempts=%s,
                http_status=%s, cta_error_code=%s, source_file=%s, object_key=%s,
                completed_at=NOW(), duration_seconds=%s
            WHERE run_id=%s AND station_id=%s
        """, (
            attempt_count, http_status, cta_error_code, source_file, object_key,
            duration_seconds, self.run_id, station_id,
        ))

    def station_failed(
        self, station_id, *, stage, attempt_count, error_type, http_status,
        cta_error_code, storage_error_code, storage_http_status, duration_seconds,
        source_file=None, object_key=None,
    ):
        self._execute("station_failed", """
            UPDATE public.collection_attempts
            SET status='failed', stage=%s, request_attempts=%s, error_type=%s,
                http_status=%s, cta_error_code=%s,
                storage_error_code=%s, storage_http_status=%s,
                source_file=%s, object_key=%s,
                completed_at=NOW(), duration_seconds=%s
            WHERE run_id=%s AND station_id=%s
        """, (
            stage, attempt_count, error_type, http_status, cta_error_code,
            storage_error_code, storage_http_status, source_file, object_key,
            duration_seconds,
            self.run_id, station_id,
        ))

    def finish(self, *, uploaded_count, failed_count, fatal_error_type=None, not_attempted_reason=None):
        if not self.enabled:
            return
        if fatal_error_type:
            status = "failed"
        elif failed_count == 0 and uploaded_count == self.expected_station_count:
            status = "completed"
        elif uploaded_count:
            status = "partial"
        else:
            status = "failed"
        if not self._execute("run_finish", """
            UPDATE public.collection_attempts
            SET status='not_attempted', stage='not_started', error_type=%s,
                completed_at=NOW()
            WHERE run_id=%s AND status='pending'
        """, (fatal_error_type or not_attempted_reason, self.run_id)):
            return
        if not self._execute("run_finish", """
            UPDATE public.collection_runs
            SET status=%s, completed_at=NOW(), uploaded_station_count=%s,
                failed_station_count=%s, fatal_error_type=%s
            WHERE run_id=%s AND status='running'
        """, (status, uploaded_count, failed_count, fatal_error_type, self.run_id)):
            return
        self.logger.info(
            "event=collection_run_completed run_id=%s status=%s expected=%s uploaded=%s failed=%s fatal_error_type=%s",
            self.run_id, status, self.expected_station_count, uploaded_count,
            failed_count, fatal_error_type,
        )

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None