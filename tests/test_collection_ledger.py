import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.storage.collection_ledger import CollectionAttemptLedger


class CollectionAttemptLedgerTests(unittest.TestCase):
    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_retry_reason_survives_successful_upload(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = connect.return_value
        connection.execute.return_value.fetchone.return_value = (2,)
        cursor = connection.cursor.return_value.__enter__.return_value
        ledger = CollectionAttemptLedger({"Wilson": 40540}, MagicMock())
        ledger.record_retry_failure(40540, http_status=503, error_type="HTTPError")
        retry_call = cursor.execute.call_args
        self.assertIn("last_retry_http_status=%s", retry_call.args[0])
        self.assertEqual(retry_call.args[1], (503, "HTTPError", ledger.run_id, 40540))
        ledger.station_uploaded(
            40540, attempt_count=2, http_status=200, cta_error_code="0",
            source_file="snapshot.xml", object_key="raw/snapshot.xml", duration_seconds=1,
        )
        self.assertNotIn("last_retry_http_status", cursor.execute.call_args.args[0])
        self.assertNotIn("last_retry_error_type", cursor.execute.call_args.args[0])

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_missing_retry_migration_keeps_existing_ledger_enabled(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = connect.return_value
        connection.execute.return_value.fetchone.return_value = (0,)
        logger = MagicMock()
        ledger = CollectionAttemptLedger({"Wilson": 40540}, logger)
        cursor = connection.cursor.return_value.__enter__.return_value
        previous_calls = cursor.execute.call_count
        ledger.record_retry_failure(40540, http_status=503, error_type="HTTPError")
        self.assertEqual(cursor.execute.call_count, previous_calls)
        self.assertTrue(ledger.enabled)
        logger.warning.assert_called_once()

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_failed_finish_commit_never_logs_completion(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        for successful_finish_commits in (0, 1):
            with self.subTest(successful_finish_commits=successful_finish_commits):
                connection = MagicMock()
                connect.return_value = connection
                logger = MagicMock()
                ledger = CollectionAttemptLedger({"Wilson": 40540}, logger)
                logger.reset_mock()
                connection.commit.side_effect = [None] * successful_finish_commits + [RuntimeError("private detail")]
                ledger.finish(uploaded_count=1, failed_count=0)
                self.assertFalse(ledger.enabled)
                logger.info.assert_not_called()
                logger.error.assert_called_once()
                self.assertNotIn("private", str(logger.mock_calls))

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_registers_expected_stations_before_collection(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: {
            "DATABASE_URL": "unused",
            "RAILWAY_GIT_COMMIT_SHA": "abc123",
        }.get(key, default)
        connection = connect.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        logger = MagicMock()

        ledger = CollectionAttemptLedger({"Wilson": 40540, "Loyola": 41300}, logger)

        self.assertTrue(ledger.enabled)
        self.assertEqual(cursor.execute.call_count, 1)
        self.assertIn("collection_runs", cursor.execute.call_args.args[0])
        self.assertEqual(cursor.executemany.call_count, 1)
        attempt_rows = cursor.executemany.call_args.args[1]
        self.assertEqual(attempt_rows, [
            (ledger.run_id, 40540, "Wilson", 1),
            (ledger.run_id, 41300, "Loyola", 2),
        ])
        connection.commit.assert_called_once()
        self.assertIn("event=collection_run_started", logger.info.call_args.args[0])

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_persists_failure_stage_and_finishes_partial_run(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = connect.return_value
        cursor = connection.cursor.return_value.__enter__.return_value
        logger = MagicMock()
        ledger = CollectionAttemptLedger({"Wilson": 40540}, logger)
        started = datetime(2026, 10, 19, 20, 0, tzinfo=timezone.utc)

        ledger.begin_station(40540, started_at=started)
        ledger.record_request_attempt(40540, 2)
        ledger.set_stage(40540, "bucket_upload", http_status=200, cta_error_code="0")
        ledger.station_failed(
            40540, stage="bucket_upload", attempt_count=2,
            error_type="RuntimeError", http_status=200, cta_error_code="0",
            storage_error_code="AccessDenied", storage_http_status=403,
            duration_seconds=1.2, source_file="snapshot.xml", object_key="raw/snapshot.xml",
        )
        ledger.finish(uploaded_count=0, failed_count=1)

        self.assertEqual(cursor.execute.call_count, 7)
        statements = [call.args[0] for call in cursor.execute.call_args_list]
        self.assertIn("request_attempts=%s", statements[2])
        self.assertIn("storage_error_code=%s", statements[4])
        self.assertIn("status=%s", statements[-1])
        self.assertIn("event=collection_run_completed", logger.info.call_args.args[0])
        ledger.close()
        connection.close.assert_called_once()

    @patch("src.storage.collection_ledger.psycopg.connect", side_effect=RuntimeError("secret connection string"))
    @patch("src.storage.collection_ledger.os.getenv")
    def test_ledger_unavailable_is_logged_safely_and_does_not_raise(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        logger = MagicMock()

        ledger = CollectionAttemptLedger({"Wilson": 40540}, logger)
        ledger.begin_station(40540, started_at=datetime.now(timezone.utc))
        ledger.finish(uploaded_count=0, failed_count=0)

        self.assertFalse(ledger.enabled)
        logger.error.assert_called_once()
        self.assertIn("error_type=%s", logger.error.call_args.args[0])
        self.assertNotIn("secret", str(logger.mock_calls))
        connect.assert_called_once()


if __name__ == "__main__":
    unittest.main()