"""Durable holding area for validated CTA XML whose bucket upload failed.

Rows are archived to the bucket and deleted only after the bucket copy's checksum
matches; the normal recovery inventory then loads them like any other archived file.
"""
import gzip
import hashlib
import os

import psycopg

from src.storage.collection_ledger import DATABASE_TIMEOUT_OPTIONS
from src.storage.object_storage import storage_error_details

MAX_ROWS = max(1, int(os.getenv("PENDING_UPLOAD_MAX_ROWS", "40000")))
MISSING_OBJECT_CODES = {"NoSuchKey", "404", "NotFound"}


class ArchiveConflict(Exception):
    """A different object already exists at the archive key."""


def _sha256(data):
    return hashlib.sha256(data).hexdigest()


class PendingUploadStore:
    """Lazily connects once per collection run; every failure is non-fatal."""

    def __init__(self, logger):
        self.logger = logger
        self.connection = None
        self.available = True

    def defer(self, path, object_key):
        """Return True only if this file's exact bytes are durably held for later archiving."""
        if not self.available:
            return False
        source_file = path.name
        try:
            xml_bytes = path.read_bytes()
        except OSError as exc:
            self.logger.error("event=pending_upload_rejected file=%r reason=unreadable error_type=%s", source_file, type(exc).__name__)
            return False
        digest = _sha256(xml_bytes)
        try:
            if self.connection is None:
                database_url = os.getenv("DATABASE_URL")
                if not database_url:
                    raise ValueError("DATABASE_URL is not configured")
                self.connection = psycopg.connect(
                    database_url, connect_timeout=10, options=DATABASE_TIMEOUT_OPTIONS,
                )
            stored = self.connection.execute("""
                INSERT INTO public.pending_uploads (source_file, object_key, xml_gzip, sha256)
                SELECT %s, %s, %s, %s
                WHERE (SELECT COUNT(*) FROM public.pending_uploads) < %s
                ON CONFLICT (source_file) DO NOTHING
                RETURNING sha256
            """, (source_file, object_key, gzip.compress(xml_bytes), digest, MAX_ROWS)).fetchone()
            if stored is None:
                stored = self.connection.execute(
                    "SELECT sha256 FROM public.pending_uploads WHERE source_file = %s", (source_file,),
                ).fetchone()
            self.connection.commit()
        except Exception as exc:
            # Stop retrying this run so an unreachable database cannot stall every station.
            self.logger.error("event=pending_upload_unavailable file=%r error_type=%s", source_file, type(exc).__name__)
            self.close()
            self.available = False
            return False
        if stored is None:
            self.logger.error("event=pending_upload_rejected file=%r reason=capacity max_rows=%s", source_file, MAX_ROWS)
            return False
        if stored[0] != digest:
            self.logger.error("event=pending_upload_rejected file=%r reason=checksum_conflict", source_file)
            return False
        self.logger.warning("event=pending_upload_stored file=%r", source_file)
        return True

    def close(self):
        if self.connection is not None:
            try:
                self.connection.close()
            except Exception:
                pass
            self.connection = None


def _archived_bytes(client, bucket, key):
    try:
        return client.get_object(Bucket=bucket, Key=key)["Body"].read()
    except Exception as exc:
        if storage_error_details(exc)[0] in MISSING_OBJECT_CODES:
            return None
        raise


def drain_pending_uploads(client, bucket, database_url, logger, *, limit, max_attempts, dry_run):
    """Archive held XML to its original bucket key, oldest first, within bounded limits."""
    stats = {"pending_archived": 0, "pending_failed": 0, "pending_remaining": 0}
    with psycopg.connect(database_url, connect_timeout=20, options=DATABASE_TIMEOUT_OPTIONS) as conn:
        if not conn.execute("SELECT to_regclass('public.pending_uploads') IS NOT NULL").fetchone()[0]:
            return stats
        rows = conn.execute("""
            SELECT source_file, object_key, xml_gzip, sha256
            FROM public.pending_uploads
            WHERE attempts < %s
            ORDER BY created_at
            LIMIT %s
        """, (max_attempts, limit)).fetchall()
        if dry_run:
            stats["pending_remaining"] = conn.execute("SELECT COUNT(*) FROM public.pending_uploads").fetchone()[0]
            logger.info("event=pending_uploads_dry_run eligible=%s total=%s", len(rows), stats["pending_remaining"])
            return stats

        for source_file, key, xml_gzip, digest in rows:
            try:
                if not key.startswith("raw/") or not key.endswith("/" + source_file):
                    raise ValueError("Pending upload has an unexpected archive key.")
                body = gzip.decompress(bytes(xml_gzip))
                if _sha256(body) != digest:
                    raise ValueError("Pending upload bytes do not match their checksum.")
                archived = _archived_bytes(client, bucket, key)
                if archived is None:
                    client.put_object(Bucket=bucket, Key=key, Body=body)
                    archived = _archived_bytes(client, bucket, key)
                if archived is None or _sha256(archived) != digest:
                    # Never overwrite or discard a differing archive object.
                    raise ArchiveConflict()
                conn.execute(
                    "DELETE FROM public.pending_uploads WHERE source_file = %s AND sha256 = %s",
                    (source_file, digest),
                )
                conn.commit()
                stats["pending_archived"] += 1
                logger.info("event=pending_upload_archived file=%r", source_file)
            except Exception as exc:
                conn.rollback()
                conn.execute("""
                    UPDATE public.pending_uploads
                    SET attempts = attempts + 1, last_error_type = %s, updated_at = NOW()
                    WHERE source_file = %s
                """, (type(exc).__name__, source_file))
                conn.commit()
                stats["pending_failed"] += 1
                code, status = storage_error_details(exc)
                logger.error(
                    "event=pending_upload_failed file=%r error_type=%s storage_error_code=%s storage_http_status=%s",
                    source_file, type(exc).__name__, code, status,
                )
        stats["pending_remaining"] = conn.execute("SELECT COUNT(*) FROM public.pending_uploads").fetchone()[0]
    log = logger.error if stats["pending_failed"] else logger.info
    log(
        "event=pending_uploads_drained archived=%s failed=%s remaining=%s",
        stats["pending_archived"], stats["pending_failed"], stats["pending_remaining"],
    )
    return stats
