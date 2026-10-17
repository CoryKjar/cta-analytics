import unittest
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.monitoring.health_check import collect_health


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


if __name__ == "__main__":
    unittest.main()