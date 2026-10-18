import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dashboard.manifest_history import (
    VALIDATION_COLUMNS, build_validation_history_frame, fetch_validation_history,
)


def history_row(hour, **counts):
    values = {"hour_utc": pd.Timestamp(hour), **counts}
    return tuple(values.get(name, 0) for name in VALIDATION_COLUMNS)


class ValidationHistoryTests(unittest.TestCase):
    def test_missing_hours_are_zero_filled_and_partial_hours_excluded(self):
        rows = [history_row("2026-10-05T17:00Z", snapshot_files=4), history_row("2026-10-05T19:00Z", snapshot_files=6)]
        frame = build_validation_history_frame(rows, "2026-10-05T17:40Z", "2026-10-05T19:30Z", 1)
        self.assertEqual(frame["Snapshots"].tolist(), [4, 0, 6])
        self.assertTrue(pd.isna(frame.loc[0, "Coverage (%)"]))
        self.assertEqual(frame.loc[1, "Coverage (%)"], 0)
        self.assertTrue(pd.isna(frame.loc[2, "Coverage (%)"]))
        self.assertTrue(frame.loc[0, "Hour"].endswith("12:00 CDT"))

    def test_unknown_flags_are_not_counted_as_false(self):
        rows = [history_row("2026-10-06T05:00Z", snapshot_files=12, prediction_rows=10,
                            delayed_rows=2, not_delayed_rows=6, unknown_delay_rows=2,
                            missing_gps_rows=3, zero_gps_rows=1)]
        frame = build_validation_history_frame(rows, "2026-10-06T05:00Z", "2026-10-06T06:00Z", 1)
        self.assertEqual(frame.loc[0, "Coverage (%)"], 100)
        self.assertEqual(frame.loc[0, "Reported delayed (%)"], 25)
        self.assertEqual(frame.loc[0, "Unknown delay (%)"], 20)
        self.assertEqual(frame.loc[0, "Missing GPS (%)"], 30)
        self.assertEqual(frame.loc[0, "Zero GPS (%)"], 10)
        self.assertTrue(pd.isna(frame.loc[1, "Reported delayed (%)"]))

    def test_empty_history_and_empty_snapshots_remain_distinct(self):
        self.assertTrue(build_validation_history_frame([], None, "2026-10-06T06:00Z", 1).empty)
        rows = [history_row("2026-10-06T05:00Z", snapshot_files=12, empty_files=12)]
        frame = build_validation_history_frame(rows, "2026-10-06T05:00Z", "2026-10-06T06:00Z", 1)
        self.assertEqual(frame.loc[0, "Empty snapshots"], 12)
        self.assertEqual(frame.loc[0, "Coverage (%)"], 100)
        self.assertTrue(pd.isna(frame.loc[0, "Reported delayed (%)"]))

    def test_fall_back_hours_have_distinct_chicago_labels(self):
        rows = [history_row("2026-11-01T06:00Z", snapshot_files=12), history_row("2026-11-01T07:00Z", snapshot_files=12)]
        frame = build_validation_history_frame(rows, "2026-11-01T06:00Z", "2026-11-01T08:00Z", 1)
        self.assertTrue(frame.loc[0, "Hour"].endswith("01:00 CDT"))
        self.assertTrue(frame.loc[1, "Hour"].endswith("01:00 CST"))

    @patch("dashboard.manifest_history.connect")
    def test_fetch_uses_read_only_consistent_snapshot(self, connect):
        conn = connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (None, pd.Timestamp("2026-10-06T06:00Z"))
        conn.execute.return_value.fetchall.return_value = []
        self.assertTrue(fetch_validation_history().empty)
        self.assertEqual(conn.execute.call_args_list[0].args[0], "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    def test_dashboard_renders_all_validation_charts(self):
        from streamlit.testing.v1 import AppTest

        frame = build_validation_history_frame(
            [history_row("2026-10-06T05:00Z", snapshot_files=12, prediction_rows=100, not_delayed_rows=100)],
            "2026-10-06T05:00Z", "2026-10-06T07:00Z", 1,
        )
        health = {
            "issues": {}, "latest_snapshot": pd.Timestamp.now(tz="UTC"),
            "age_minutes": 1, "status_counts": {"processed": 12},
            "recent_updates": 12, "mismatch_count": 0,
            "nonprocessed_rows": [], "retry_rows": [],
        }
        with patch.dict("os.environ", {"DATABASE_URL": "unused"}), \
                patch("src.monitoring.health_check.collect_health", return_value=health), \
                patch("dashboard.manifest_history.fetch_validation_history", return_value=frame), \
                patch("dashboard.manifest_history.fetch_manifest_history", return_value=pd.DataFrame()):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")).run(timeout=30)
        self.assertFalse(app.exception)
        self.assertEqual([item.label for item in app.tabs], ["Coverage", "Consistency", "Source fields"])
        charts = app.get("vega_lite_chart") or app.get("arrow_vega_lite_chart")
        self.assertEqual(len(charts), 7)


if __name__ == "__main__":
    unittest.main()