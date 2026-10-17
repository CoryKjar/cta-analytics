"""Shared lossless audit writes for live ingestion and isolated rebuilds."""
import gzip
import hashlib
import json

from psycopg import sql
from psycopg.types.json import Jsonb

from src.storage.resolve_rebuild_conflicts import POLICY


def ensure_audit_table(conn, destination, reference):
    conn.execute(sql.SQL("""
        CREATE TABLE IF NOT EXISTS {} (
            file_id integer PRIMARY KEY REFERENCES {}(file_id),
            policy text NOT NULL,source_sha256 text NOT NULL,source_etag text,
            source_xml_gzip bytea NOT NULL,evidence jsonb NOT NULL,
            canonical_row_count integer NOT NULL,resolved_at timestamptz NOT NULL DEFAULT NOW()
        )
    """).format(destination, reference))


def write_quality_audit(cursor, destination, record, rows, data, decisions, *, etag=None):
    """Never replace audit history or accept changed source bytes on replay."""
    digest = hashlib.sha256(data).hexdigest()
    evidence = {"policy": POLICY, "source_file": record[1], "object_key": record[4], "decisions": decisions}
    normalized = json.loads(json.dumps(evidence, default=str, allow_nan=False))
    cursor.execute(sql.SQL("SELECT policy,source_sha256,canonical_row_count,evidence FROM {} WHERE file_id=%s FOR UPDATE").format(destination), (record[0],))
    prior = cursor.fetchone()
    if prior:
        if prior != (POLICY, digest, len(rows), normalized):
            raise ValueError("Existing quality audit differs from replayed source; refusing to overwrite history.")
        return
    cursor.execute(sql.SQL("""
        INSERT INTO {} (file_id,policy,source_sha256,source_etag,source_xml_gzip,evidence,canonical_row_count)
        VALUES (%s,%s,%s,%s,%s,%s,%s)
    """).format(destination), (record[0], POLICY, digest, etag, gzip.compress(data), Jsonb(normalized), len(rows)))