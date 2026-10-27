import gzip
import hashlib
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

from botocore.exceptions import ClientError

from src.storage.pending_uploads import PendingUploadStore, drain_pending_uploads

NAME = "20261021_182500_Wilson.xml"
KEY = "raw/2026/10/21/" + NAME
XML = b"<ctatt><tmst>20261021 18:25:00</tmst><errCd>0</errCd></ctatt>"
DIGEST = hashlib.sha256(XML).hexdigest()


def client_error(code, status):
    return ClientError({"Error": {"Code": code, "Message": "private"}, "ResponseMetadata": {"HTTPStatusCode": status}}, "op")


class FakeBucket:
    def __init__(self, objects=None, put_error=None):
        self.objects = dict(objects or {})
        self.put_error = put_error
        self.puts = 0

    def get_object(self, Bucket, Key):
        if Key not in self.objects:
            raise client_error("NoSuchKey", 404)
        body = MagicMock()
        body.read.return_value = self.objects[Key]
        return {"Body": body}

    def put_object(self, Bucket, Key, Body):
        self.puts += 1
        if self.put_error:
            raise self.put_error
        self.objects[Key] = Body


class FakeHoldingTable:
    def __init__(self, rows, exists=True):
        self.rows = {row["source_file"]: row for row in rows}
        self.exists = exists

    def __enter__(self):
        return self

    def __exit__(self, *args):
        return False

    def commit(self):
        pass

    def rollback(self):
        pass

    def execute(self, query, parameters=None):
        result = MagicMock()
        if "to_regclass" in query:
            result.fetchone.return_value = (self.exists,)
        elif "SELECT source_file, object_key" in query:
            max_attempts, limit = parameters
            eligible = [row for row in self.rows.values() if row["attempts"] < max_attempts]
            result.fetchall.return_value = [
                (row["source_file"], row["object_key"], row["xml_gzip"], row["sha256"]) for row in eligible
            ][:limit]
        elif query.startswith("DELETE"):
            name, digest = parameters
            if self.rows.get(name, {}).get("sha256") == digest:
                del self.rows[name]
        elif "UPDATE public.pending_uploads" in query:
            error_type, name = parameters
            self.rows[name]["attempts"] += 1
            self.rows[name]["last_error_type"] = error_type
        elif "COUNT(*)" in query:
            result.fetchone.return_value = (len(self.rows),)
        return result


def held(xml=XML, digest=DIGEST, attempts=0, key=KEY):
    return {"source_file": NAME, "object_key": key, "xml_gzip": gzip.compress(xml), "sha256": digest, "attempts": attempts}


class DrainPendingUploadsTests(unittest.TestCase):
    def drain(self, table, bucket, *, dry_run=False):
        logger = MagicMock()
        with patch("src.storage.pending_uploads.psycopg.connect", return_value=table):
            stats = drain_pending_uploads(bucket, "bucket", "unused", logger, limit=500, max_attempts=5, dry_run=dry_run)
        return stats, logger

    def test_held_xml_is_archived_with_identical_bytes_then_removed_once(self):
        table, bucket = FakeHoldingTable([held()]), FakeBucket()
        stats, _ = self.drain(table, bucket)
        self.assertEqual(bucket.objects, {KEY: XML})
        self.assertEqual(table.rows, {})
        self.assertEqual(stats, {"pending_archived": 1, "pending_failed": 0, "pending_remaining": 0})
        stats, _ = self.drain(table, bucket)
        self.assertEqual((stats["pending_archived"], bucket.puts), (0, 1))

    def test_bucket_still_failing_keeps_row_and_logs_safely_then_succeeds_later(self):
        table = FakeHoldingTable([held()])
        stats, logger = self.drain(table, FakeBucket(put_error=client_error("SignatureDoesNotMatch", 403)))
        self.assertEqual(stats["pending_failed"], 1)
        self.assertEqual((table.rows[NAME]["attempts"], table.rows[NAME]["last_error_type"]), (1, "ClientError"))
        self.assertIn("SignatureDoesNotMatch", str(logger.error.call_args_list))
        self.assertNotIn("private", str(logger.mock_calls))
        stats, _ = self.drain(table, FakeBucket())
        self.assertEqual(stats["pending_archived"], 1)

    def test_identical_existing_archive_is_accepted_without_reupload(self):
        table, bucket = FakeHoldingTable([held()]), FakeBucket({KEY: XML})
        self.drain(table, bucket)
        self.assertEqual((bucket.puts, table.rows), (0, {}))

    def test_different_existing_archive_is_never_overwritten(self):
        table, bucket = FakeHoldingTable([held()]), FakeBucket({KEY: b"<ctatt>other</ctatt>"})
        stats, _ = self.drain(table, bucket)
        self.assertEqual(bucket.objects[KEY], b"<ctatt>other</ctatt>")
        self.assertEqual(table.rows[NAME]["last_error_type"], "ArchiveConflict")
        self.assertEqual((stats["pending_failed"], bucket.puts), (1, 0))

    def test_corrupt_or_misaddressed_rows_are_not_uploaded(self):
        for row, error in ((held(digest="0" * 64), "ValueError"), (held(key="other/" + NAME), "ValueError")):
            with self.subTest(error=error, key=row["object_key"]):
                table, bucket = FakeHoldingTable([row]), FakeBucket()
                self.drain(table, bucket)
                self.assertEqual((bucket.objects, table.rows[NAME]["last_error_type"]), ({}, error))

    def test_retry_cap_dry_run_and_missing_table_make_no_writes(self):
        table, bucket = FakeHoldingTable([held(attempts=5)]), FakeBucket()
        stats, _ = self.drain(table, bucket)
        self.assertEqual((stats["pending_remaining"], bucket.puts), (1, 0))
        table = FakeHoldingTable([held()])
        stats, _ = self.drain(table, bucket, dry_run=True)
        self.assertEqual((stats["pending_remaining"], bucket.puts, table.rows[NAME]["attempts"]), (1, 0, 0))
        stats, _ = self.drain(FakeHoldingTable([], exists=False), bucket)
        self.assertEqual(stats, {"pending_archived": 0, "pending_failed": 0, "pending_remaining": 0})


class PendingUploadStoreTests(unittest.TestCase):
    def setUp(self):
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.path = Path(self.directory.name) / NAME
        self.path.write_bytes(XML)

    def connection(self, inserted, existing=None):
        connection = MagicMock()

        def execute(query, parameters=None):
            result = MagicMock()
            result.fetchone.return_value = inserted if "INSERT" in query else existing
            return result

        connection.execute.side_effect = execute
        return connection

    @patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"})
    @patch("src.storage.pending_uploads.psycopg.connect")
    def test_stores_exact_bytes_and_checksum(self, connect):
        connect.return_value = self.connection((DIGEST,))
        store = PendingUploadStore(MagicMock())
        self.assertTrue(store.defer(self.path, KEY))
        insert = connect.return_value.execute.call_args_list[0].args[1]
        self.assertEqual((insert[0], insert[1], gzip.decompress(insert[2]), insert[3]), (NAME, KEY, XML, DIGEST))
        connect.return_value.commit.assert_called_once()

    @patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"})
    @patch("src.storage.pending_uploads.psycopg.connect")
    def test_duplicate_is_accepted_but_capacity_or_conflict_is_rejected(self, connect):
        for existing, expected, reason in (((DIGEST,), True, None), (None, False, "capacity"), (("f" * 64,), False, "checksum_conflict")):
            with self.subTest(reason=reason):
                connect.return_value = self.connection(None, existing)
                logger = MagicMock()
                self.assertIs(PendingUploadStore(logger).defer(self.path, KEY), expected)
                if reason:
                    self.assertIn(reason, str(logger.error.call_args))

    @patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"})
    @patch("src.storage.pending_uploads.psycopg.connect", side_effect=OSError("postgresql://private:secret@host"))
    def test_database_outage_fails_fast_once_per_run_without_secrets(self, connect):
        logger = MagicMock()
        store = PendingUploadStore(logger)
        self.assertFalse(store.defer(self.path, KEY))
        self.assertFalse(store.defer(self.path, KEY))
        connect.assert_called_once()
        self.assertNotIn("secret", str(logger.mock_calls))


class PendingUploadHealthTests(unittest.TestCase):
    @patch("src.monitoring.health_check.STATIONS", {"Station": 1})
    @patch("src.monitoring.health_check.connect")
    def test_old_or_capped_held_xml_raises_issue_but_fresh_backlog_does_not(self, connect):
        from datetime import datetime, timedelta, timezone
        from src.monitoring.health_check import collect_health
        now = datetime.now(timezone.utc)
        for oldest_minutes, capped, expected in ((30, 0, False), (120, 0, True), (30, 2, True)):
            with self.subTest(oldest_minutes=oldest_minutes, capped=capped):
                def execute(query, parameters=None):
                    cursor = MagicMock()
                    cursor.fetchall.return_value = []
                    cursor.fetchone.return_value = (0,)
                    if "to_regclass('public.collection_runs')" in query:
                        cursor.fetchone.return_value = (None,)
                    elif "to_regclass('public.pending_uploads')" in query:
                        cursor.fetchone.return_value = (True,)
                    elif "FROM public.pending_uploads" in query:
                        cursor.fetchone.return_value = (5, now - timedelta(minutes=oldest_minutes), capped)
                    elif "GROUP BY station_id" in query:
                        cursor.fetchall.return_value = [(1, now)]
                    elif "MAX(collection_timestamp" in query:
                        cursor.fetchone.return_value = (now,)
                    return cursor

                connect.return_value.__enter__.return_value.execute.side_effect = execute
                health = collect_health()
                self.assertIs("pending_uploads" in health["issues"], expected)
                self.assertEqual(health["pending_upload_count"], 5)


if __name__ == "__main__":
    unittest.main()
