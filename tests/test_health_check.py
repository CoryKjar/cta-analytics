import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.monitoring.health_check import collect_health, evaluate_and_alert


class StationFreshnessTests(unittest.TestCase):
    @patch("src.monitoring.health_check.STATIONS", {"Fresh": 1, "Missing": 2})
    @patch("src.monitoring.health_check.connect")
    def test_global_freshness_does_not_hide_missing_station(self, connect):
        now = datetime.now(timezone.utc)
        conn = connect.return_value.__enter__.return_value
        def execute(query, parameters=None):
            cursor = MagicMock()
            if "GROUP BY station_id" in query:
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
        self.assertIn("event=health_issue", logger.error.call_args_list[0].args[0])
        self.assertIn("event=health_check_completed", logger.error.call_args.args[0])
        self.assertTrue(any("event=health_email_sent" in call.args[0] for call in logger.info.call_args_list))

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