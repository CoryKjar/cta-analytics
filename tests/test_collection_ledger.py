import unittest
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.storage.collection_ledger import CollectionAttemptLedger, CollectionSkipped


def answer_ledger_queries(connection, *, retry_columns=2, lock_acquired=True, interval_collected=False):
    def execute(query, parameters=None):
        result = MagicMock()
        if "pg_try_advisory_lock" in query:
            result.fetchone.return_value = (lock_acquired,)
        elif "started_at >= to_timestamp" in query:
            result.fetchone.return_value = (interval_collected,)
        else:
            result.fetchone.return_value = (retry_columns,)
        return result
    connection.execute.side_effect = execute
    return connection


class CollectionAttemptLedgerTests(unittest.TestCase):
    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_retry_reason_survives_successful_upload(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = answer_ledger_queries(connect.return_value, retry_columns=2)
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
        connection = answer_ledger_queries(connect.return_value, retry_columns=0)
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
                connection = answer_ledger_queries(MagicMock())
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
        connection = answer_ledger_queries(connect.return_value)
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
        connection = answer_ledger_queries(connect.return_value)
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

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_overlapping_or_duplicate_interval_collection_is_skipped_without_writes(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        for options, reason in (
            ({"lock_acquired": False}, "collection_in_progress"),
            ({"interval_collected": True}, "interval_already_collected"),
        ):
            with self.subTest(reason=reason):
                connection = answer_ledger_queries(MagicMock(), **options)
                connect.return_value = connection
                logger = MagicMock()
                with self.assertRaises(CollectionSkipped) as raised:
                    CollectionAttemptLedger({"Wilson": 40540}, logger)
                self.assertEqual(raised.exception.reason, reason)
                connection.cursor.assert_not_called()
                connection.commit.assert_not_called()
                connection.close.assert_called_once()
                self.assertIn("event=collection_skipped", logger.warning.call_args.args[0])

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_interval_check_allows_retry_after_failed_or_partial_run(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = answer_ledger_queries(connect.return_value)
        CollectionAttemptLedger({"Wilson": 40540}, MagicMock())
        interval_query = next(
            call.args[0] for call in connection.execute.call_args_list
            if "started_at >= to_timestamp" in call.args[0]
        )
        self.assertIn("status IN ('running', 'completed')", interval_query)
        self.assertNotIn("partial", interval_query)

    @patch("src.storage.collection_ledger.psycopg.connect")
    @patch("src.storage.collection_ledger.os.getenv")
    def test_deadline_reason_is_recorded_for_unstarted_stations(self, getenv, connect):
        getenv.side_effect = lambda key, default=None: "unused" if key == "DATABASE_URL" else default
        connection = answer_ledger_queries(connect.return_value)
        cursor = connection.cursor.return_value.__enter__.return_value
        ledger = CollectionAttemptLedger({"Wilson": 40540, "Loyola": 41300}, MagicMock())
        ledger.finish(uploaded_count=1, failed_count=0, not_attempted_reason="DeadlineExceeded")
        pending_update, run_update = cursor.execute.call_args_list[-2:]
        self.assertIn("status='not_attempted'", pending_update.args[0])
        self.assertEqual(pending_update.args[1], ("DeadlineExceeded", ledger.run_id))
        self.assertEqual(run_update.args[1][0], "partial")


class CollectorDatabaseTimeoutTests(unittest.TestCase):
    def test_collector_write_connections_bound_lock_waits_and_statements(self):
        import pandas as pd
        from src.storage import collection_ledger, load_arrivals_postgres, pending_uploads
        from src.storage.collection_ledger import DATABASE_TIMEOUT_OPTIONS

        self.assertIn("lock_timeout=30s", DATABASE_TIMEOUT_OPTIONS)
        self.assertIn("statement_timeout=120s", DATABASE_TIMEOUT_OPTIONS)
        record = {
            "source_file": "20261019_195500_Wilson.xml", "collection_timestamp": pd.Timestamp("2026-10-19 19:55"),
            "station_id": 40540, "status": "processed", "row_count": 0, "last_error": None,
        }
        with patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"}), \
                patch.object(collection_ledger.psycopg, "connect") as connect:
            answer_ledger_queries(connect.return_value)
            CollectionAttemptLedger({"Wilson": 40540}, MagicMock())
            with patch("src.storage.load_arrivals_postgres.load_dotenv"):
                load_arrivals_postgres.register_file_results([record])
            pending_uploads.PendingUploadStore(MagicMock()).defer(MagicMock(read_bytes=lambda: b"<ctatt/>"), "raw/x/y.xml")
        self.assertTrue(connect.call_args_list)
        for call in connect.call_args_list:
            self.assertEqual(call.kwargs["options"], DATABASE_TIMEOUT_OPTIONS)


if __name__ == "__main__":
    unittest.main()