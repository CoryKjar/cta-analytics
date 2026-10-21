import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.monitoring.health_check import COLLECTION_RUN_STALE_MINUTES, collect_health, evaluate_and_alert


class StationFreshnessTests(unittest.TestCase):
    @patch("src.monitoring.health_check.STATIONS", {"Fresh": 1, "Missing": 2})
    @patch("src.monitoring.health_check.connect")
    def test_global_freshness_does_not_hide_missing_station(self, connect):
        now = datetime.now(timezone.utc)
        conn = connect.return_value.__enter__.return_value
        def execute(query, parameters=None):
            cursor = MagicMock()
            if "to_regclass('public.collection_runs')" in query:
                cursor.fetchone.return_value = (None,)
            elif "GROUP BY station_id" in query:
                cursor.fetchall.return_value = [(1, now)]
            elif "MAX(collection_timestamp" in query:
                cursor.fetchone.return_value = (now,)
            elif "GROUP BY status" in query:
                cursor.fetchall.return_value = [("processed", 10)]
            else:
                cursor.fetchone.return_value = (0,)
                cursor.fetchall.return_value = []
            return cursor
        conn.execute.side_effect = execute
        health = collect_health()
        self.assertEqual(health["stale_stations"], ["Missing"])
        self.assertIn("station_collection_stale", health["issues"])
        self.assertNotIn("collection_stale", health["issues"])
        self.assertEqual(health["stale_collection_runs"], [])
        self.assertNotIn("partial_collection_run", health["issues"])

    @patch("src.monitoring.health_check.STATIONS", {"Station": 1})
    @patch("src.monitoring.health_check.connect")
    def test_stale_and_partial_collection_runs_are_reported(self, connect):
        now = datetime.now(timezone.utc)
        conn = connect.return_value.__enter__.return_value
        retry_column_count = 0

        def execute(query, parameters=None):
            cursor = MagicMock()
            if "to_regclass('public.collection_runs')" in query:
                cursor.fetchone.return_value = ("collection_runs",)
            elif "information_schema.columns" in query:
                cursor.fetchone.return_value = (retry_column_count,)
            elif "FROM public.collection_runs AS r" in query and "LIMIT 20" in query:
                cursor.fetchall.return_value = [(
                    "stale-run-1234", now - timedelta(minutes=20), None, "running",
                    143, 58, 1, None, 83, 1,
                )]
            elif "FROM public.collection_runs AS r" in query:
                cursor.fetchall.return_value = [("stale-run-1234", now - timedelta(minutes=20), 143, 58, 1, 84)]
            elif "FROM public.collection_runs" in query:
                cursor.fetchall.return_value = [("partial-run-5678", now - timedelta(minutes=4), "partial", 143, 58, 85, None)]
            elif "GROUP BY station_id" in query:
                cursor.fetchall.return_value = [(1, now)]
            elif "MAX(collection_timestamp" in query:
                cursor.fetchone.return_value = (now,)
            elif "GROUP BY status" in query:
                cursor.fetchall.return_value = [("processed", 10)]
            else:
                cursor.fetchone.return_value = (0,)
                cursor.fetchall.return_value = []
            return cursor

        conn.execute.side_effect = execute
        for window_hours, retry_column_count in ((24, 0), (720, 2), (None, 2), (None, 0)):
            with self.subTest(window_hours=window_hours, retry_column_count=retry_column_count):
                conn.execute.reset_mock()
                health = collect_health(collection_window_hours=window_hours)

                self.assertIn("collection_run_incomplete", health["issues"])
                self.assertIn("partial_collection_run", health["issues"])
                self.assertIn("stale-ru", health["issues"]["collection_run_incomplete"])
                self.assertIn("no recorded completion", health["issues"]["collection_run_incomplete"])
                self.assertIn("process activity unverified", health["issues"]["collection_run_incomplete"])
                self.assertNotIn("still running after", health["issues"]["collection_run_incomplete"])
                self.assertIn("58/143 uploaded", health["issues"]["partial_collection_run"])
                self.assertEqual(health["recent_collection_runs"][0][5:7], (58, 1))
                self.assertEqual(health["has_collection_retry_details"], retry_column_count == 2)
                log_calls = [call for call in conn.execute.call_args_list
                             if "%s::integer IS NULL" in call.args[0]]
                self.assertEqual(len(log_calls), 3)
                for call in log_calls:
                    self.assertEqual(call.args[1], (window_hours, window_hours))
                    self.assertNotIn("INTERVAL '24 hours'", call.args[0])
                self.assertTrue(any("OR attempts > 0" in call.args[0] for call in log_calls))
                self.assertTrue(any("OR a.request_attempts > 1" in call.args[0] for call in log_calls))
                run_queries = [call.args[0] for call in log_calls if "GROUP BY r.run_id" in call.args[0]]
                self.assertEqual(len(run_queries), 1)
                self.assertIn("COUNT(*) FILTER (WHERE a.status = 'uploaded')", run_queries[0])
                self.assertIn("COUNT(*) FILTER (WHERE a.status = 'failed')", run_queries[0])
                self.assertNotIn("r.uploaded_station_count", run_queries[0])
                attempt_query = next(call.args[0] for call in log_calls if "a.request_attempts > 1" in call.args[0])
                if retry_column_count == 2:
                    self.assertIn("a.last_retry_http_status, a.last_retry_error_type", attempt_query)
                else:
                    self.assertIn("NULL::integer, NULL::text", attempt_query)
                    self.assertNotIn("a.last_retry_http_status", attempt_query)
                partial_calls = [call for call in conn.execute.call_args_list
                                 if "WHERE status IN ('partial', 'failed')" in call.args[0]]
                self.assertEqual(len(partial_calls), 1)
                self.assertEqual(partial_calls[0].args[1], (COLLECTION_RUN_STALE_MINUTES,))
                self.assertNotIn("INTERVAL '24 hours'", partial_calls[0].args[0])


class HealthLoggingTests(unittest.TestCase):
    def setUp(self):
        self.health = {
            "checked_at": datetime.now(timezone.utc),
            "latest_snapshot": datetime.now(timezone.utc),
            "issues": {}, "recent_updates": 143,
            "stale_stations": [], "mismatch_count": 0,
        }

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.send_email")
    @patch("src.monitoring.health_check.connect")
    @patch("src.monitoring.health_check.collect_health")
    def test_healthy_check_uses_info_and_baseline_sends_no_email(self, collect, connect, send, logger):
        collect.return_value = self.health
        connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = ("",)
        self.assertIs(evaluate_and_alert(), self.health)
        self.assertIn("event=health_check_completed", logger.info.call_args.args[0])
        logger.error.assert_not_called()
        send.assert_not_called()

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.send_email")
    @patch("src.monitoring.health_check.connect")
    @patch("src.monitoring.health_check.collect_health")
    def test_issues_use_error_and_successful_alert_is_logged(self, collect, connect, send, logger):
        self.health["issues"] = {"collection_stale": "Latest snapshot is stale"}
        collect.return_value = self.health
        connect.return_value.__enter__.return_value.execute.return_value.fetchone.return_value = ("",)
        evaluate_and_alert()
        send.assert_called_once()
        self.assertIn("Latest snapshot is stale", send.call_args.args[1])
        self.assertIn("event=health_issue", logger.error.call_args_list[0].args[0])
        self.assertIn("detail=%s", logger.error.call_args_list[0].args[0])
        self.assertIn("event=health_check_completed", logger.error.call_args.args[0])
        self.assertTrue(any("event=health_email_sent" in call.args[0] for call in logger.info.call_args_list))

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.send_email")
    @patch("src.monitoring.health_check.connect")
    @patch("src.monitoring.health_check.collect_health")
    def test_incomplete_run_alert_contains_details_and_unchanged_issue_stays_logged(self, collect, connect, send, logger):
        detail = "Run 63c07775 has no recorded completion: 72/143 uploaded, 71 unfinished."
        self.health["issues"] = {"collection_run_incomplete": detail}
        collect.return_value = self.health
        conn = connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("",)
        evaluate_and_alert()
        self.assertIn(detail, send.call_args.args[1])
        self.assertIn(detail, logger.error.call_args_list[0].args)
        conn.execute.return_value.fetchone.return_value = ("collection_run_incomplete",)
        send.reset_mock()
        logger.reset_mock()
        evaluate_and_alert()
        send.assert_not_called()
        self.assertIn(detail, logger.error.call_args_list[0].args)
        self.assertIn("event=health_check_completed", logger.error.call_args.args[0])

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.send_email")
    @patch("src.monitoring.health_check.connect")
    @patch("src.monitoring.health_check.collect_health")
    def test_cleared_run_issue_sends_recovery_before_clearing_state(self, collect, connect, send, logger):
        collect.return_value = self.health
        conn = connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("collection_run_incomplete",)
        evaluate_and_alert()
        send.assert_called_once()
        self.assertEqual(send.call_args.args[0], "[CTA RECOVERED] Pipeline health is clear")
        self.assertIn("No monitored issues", send.call_args.args[1])
        self.assertEqual(conn.execute.call_args.args[1], ("pipeline_health", "", True, True))
        logger.error.assert_not_called()

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.collect_health", side_effect=RuntimeError("postgresql://private:secret@host/db"))
    def test_failed_check_logs_only_error_type_and_still_raises(self, collect, logger):
        with self.assertRaises(RuntimeError):
            evaluate_and_alert()
        logger.error.assert_called_once_with("event=health_check_failed error_type=%s", "RuntimeError")
        self.assertNotIn("secret", str(logger.mock_calls))

    @patch("src.monitoring.health_check.logger")
    @patch("src.monitoring.health_check.send_email", side_effect=RuntimeError("smtp://private:secret@host"))
    @patch("src.monitoring.health_check.connect")
    @patch("src.monitoring.health_check.collect_health")
    def test_email_failure_is_logged_without_credentials_or_state_advance(self, collect, connect, send, logger):
        self.health["issues"] = {"collection_stale": "Latest snapshot is stale"}
        collect.return_value = self.health
        conn = connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = ("",)
        with self.assertRaises(RuntimeError):
            evaluate_and_alert()
        self.assertEqual(conn.execute.call_count, 1)
        self.assertIn("event=health_check_failed", logger.error.call_args.args[0])
        self.assertNotIn("secret", str(logger.mock_calls))


if __name__ == "__main__":
    unittest.main()