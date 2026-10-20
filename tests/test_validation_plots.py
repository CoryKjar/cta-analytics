import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd

from dashboard.manifest_history import (
    VALIDATION_COLUMNS, build_route_quality_frame, build_station_coverage_frame,
    build_validation_history_frame, fetch_dashboard_history, fetch_validation_history,
)
from src.config.stations import STATIONS


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
        frame = build_validation_history_frame(rows, "2026-10-06T05:00Z", "2026-10-06T06:30Z", 1)
        self.assertEqual(frame.loc[0, "Coverage (%)"], 100)
        self.assertEqual(frame.loc[0, "Reported delayed (%)"], 25)
        self.assertEqual(frame.loc[0, "Unknown delay (%)"], 20)
        self.assertEqual(frame.loc[0, "Missing GPS (%)"], 30)
        self.assertEqual(frame.loc[0, "Zero GPS (%)"], 10)
        self.assertTrue(pd.isna(frame.loc[1, "Reported delayed (%)"]))

    def test_missing_gps_rates_are_split_by_prediction_type(self):
        rows = [history_row(
            "2026-10-06T05:00Z", snapshot_files=12, prediction_rows=100,
            scheduled_rows=40, live_rows=50, unknown_schedule_rows=10,
            scheduled_missing_gps_rows=20, live_missing_gps_rows=5,
            unclassified_missing_gps_rows=10,
        )]
        frame = build_validation_history_frame(
            rows, "2026-10-06T05:00Z", "2026-10-06T06:00Z", 1,
        )
        self.assertEqual(frame.loc[0, "Scheduled missing GPS (%)"], 50)
        self.assertEqual(frame.loc[0, "Live missing GPS (%)"], 10)
        self.assertEqual(frame.loc[0, "Unclassified missing GPS (%)"], 100)

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

    def test_station_coverage_includes_configured_stations_with_no_files(self):
        first_id = next(iter(STATIONS.values()))
        frame = build_station_coverage_frame(
            [(first_id, 287, 1, 2, pd.Timestamp("2026-10-06 00:00:00"))],
            "2026-10-05T05:00Z", "2026-10-06T05:00Z",
        )
        self.assertEqual(len(frame), len(STATIONS))
        self.assertAlmostEqual(frame.loc[frame["Station ID"] == first_id, "Coverage (%)"].iloc[0], 287 / 288 * 100)
        self.assertEqual(frame.iloc[0]["Snapshots"], 0)
        self.assertEqual(frame.iloc[0]["Coverage (%)"], 0)

    def test_route_delay_rate_excludes_unknown_flag_values(self):
        frame = build_route_quality_frame([("Red", 10, 8.0, 18.0, 2, 6, 2)])
        self.assertEqual(frame.loc[0, "Reported delayed (%)"], 25)
        self.assertEqual(frame.loc[0, "Unknown delay (%)"], 20)

    @patch("dashboard.manifest_history.connect")
    def test_fetch_uses_read_only_consistent_snapshot(self, connect):
        conn = connect.return_value.__enter__.return_value
        conn.execute.return_value.fetchone.return_value = (None, pd.Timestamp("2026-10-06T06:00Z"))
        conn.execute.return_value.fetchall.return_value = []
        self.assertTrue(fetch_validation_history().empty)
        self.assertEqual(conn.execute.call_args_list[0].args[0], "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY")

    @patch("dashboard.manifest_history.connect")
    def test_window_and_route_station_panels_share_one_read_only_snapshot(self, connect):
        conn = connect.return_value.__enter__.return_value
        first = pd.Timestamp("2026-10-05T05:00Z")
        as_of = pd.Timestamp("2026-10-12T05:00Z")

        def execute(query, parameters=None):
            cursor = unittest.mock.MagicMock()
            if query.startswith("SELECT MIN(collection_timestamp"):
                cursor.fetchone.return_value = (first, as_of)
            elif query == VALIDATION_HISTORY_QUERY:
                cursor.fetchall.return_value = []
            elif query.startswith("\n    SELECT r.route_name"):
                cursor.fetchall.return_value = [("Red", 20, 10.0, 20.0, 2, 16, 2)]
            else:
                cursor.fetchall.return_value = [(next(iter(STATIONS.values())), 1008, 0, 4, first)]
            return cursor

        from dashboard.manifest_history import VALIDATION_HISTORY_QUERY

        conn.execute.side_effect = execute
        result = fetch_dashboard_history(168)
        self.assertEqual(result["window_start"], first)
        self.assertEqual(len(result["routes"]), 1)
        self.assertEqual(result["routes"].loc[0, "Reported delayed (%)"], 11.11111111111111)
        bounded_calls = [call for call in conn.execute.call_args_list if call.args[0] in (VALIDATION_HISTORY_QUERY,)]
        self.assertEqual(len(bounded_calls), 1)
        self.assertEqual(bounded_calls[0].args[1], (first.to_pydatetime(), as_of.to_pydatetime()))
        self.assertEqual(result["stations"].loc[result["stations"]["Station ID"] == next(iter(STATIONS.values())), "Snapshots"].iloc[0], 1008)

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
            "file_issue_logs": [
                ("broken.xml", "failed", 1, "parse: ParseError", pd.Timestamp("2026-10-06T06:00Z")),
                ("recovered.xml", "processed", 2, None, pd.Timestamp("2026-10-06T06:00Z")),
                ("legacy.xml", "failed", 1, "private error with token=secret", pd.Timestamp("2026-10-06T06:00Z")),
            ],
            "has_collection_ledger": True,
            "recent_collection_runs": [(
                "run-12345678", pd.Timestamp("2026-10-06T06:00Z"),
                pd.Timestamp("2026-10-06T06:01Z"), "partial", 143, 142, 1,
                None, 0, 0,
            )],
            "collection_attempt_logs": [(
                "run-12345678", pd.Timestamp("2026-10-06T06:00Z"),
                "Wilson", 40540, 143, "failed", "bucket_upload", 1, 200,
                None, "AccessDenied", 403, "ClientError",
                "snapshot.xml", "raw/2026/10/06/snapshot.xml", 0.5,
            )],
        }
        with patch.dict("os.environ", {"DATABASE_URL": "unused"}), \
            patch("src.monitoring.health_check.collect_health", return_value=health) as collect, \
                patch("dashboard.manifest_history.fetch_dashboard_history", return_value={
                    "hourly": frame,
                    "routes": pd.DataFrame({
                        "Route": ["Red"], "Predictions": [100],
                        "Median predicted wait (min)": [8.0],
                        "P90 predicted wait (min)": [15.0],
                        "Reported delayed": [0], "Reported not delayed": [100],
                        "Unknown delay flags": [0], "Reported delayed (%)": [0.0],
                        "Unknown delay (%)": [0.0],
                    }),
                    "stations": pd.DataFrame({
                        "Station": ["Wilson"], "Station ID": [40540],
                        "Snapshots": [12], "Expected snapshots": [12.0],
                        "Coverage (%)": [100.0], "Unprocessed files": [0],
                        "Empty snapshots": [0], "Latest snapshot": [pd.Timestamp.now()],
                    }),
                    "window_start": pd.Timestamp("2026-10-06T05:00Z"),
                    "as_of": pd.Timestamp("2026-10-06T07:00Z"),
                }), \
                patch("dashboard.manifest_history.fetch_manifest_history", return_value=pd.DataFrame()):
            app = AppTest.from_file(str(Path(__file__).resolve().parents[1] / "dashboard" / "app.py")).run(timeout=30)
            collect.assert_called_with(collection_window_hours=168)
            file_logs = [item.value for item in app.dataframe if "Failed attempts" in item.value.columns]
            self.assertEqual(len(file_logs), 1)
            self.assertEqual(file_logs[0]["Failed attempts"].tolist(), [1, 2, 1])
            self.assertEqual(file_logs[0]["Last recorded error"].tolist(), [
                "parse: ParseError", "Recovered; previous error not retained",
                "Stored error details unavailable in safe format",
            ])
            attempt_logs = [item.value for item in app.dataframe if "Request attempts" in item.value.columns]
            self.assertEqual(attempt_logs[0]["Storage error"].tolist(), ["AccessDenied"])
            self.assertEqual(attempt_logs[0]["Stage"].tolist(), ["bucket_upload"])
            health["recent_collection_runs"] = []
            health["collection_attempt_logs"] = []
            for label, hours, period in (
                ("30 days", 720, "in the last 30 days"),
                ("All history", None, "across all history"),
            ):
                with self.subTest(window=label):
                    app.selectbox[0].select(label).run(timeout=30)
                    self.assertFalse(app.exception)
                    collect.assert_called_with(collection_window_hours=hours)
                    self.assertIn(f"No collection runs recorded {period}.", [item.value for item in app.caption])
                    self.assertIn(
                        f"No collection failures or request retries recorded {period}.",
                        [item.value for item in app.info],
                    )
                    self.assertFalse(any("below nominal" in item.value for item in app.warning))
                    self.assertFalse(any("Below nominal" in item.value.columns for item in app.dataframe))
            health["has_collection_ledger"] = False
            app.run(timeout=30)
            self.assertTrue(any("Failed attempts" in item.value.columns for item in app.dataframe))
        self.assertFalse(app.exception)
        self.assertEqual(app.selectbox[0].options, ["24 hours", "7 days", "30 days", "All history"])
        self.assertEqual([item.label for item in app.tabs], ["Overview", "Collection", "Forecast behavior", "Data quality"])
        self.assertIn("Collection logs", [item.value for item in app.subheader])
        self.assertGreaterEqual(len(app.get("vega_lite_chart")), 7)
        self.assertIn("Clear", [item.value for item in app.metric])
        charts = app.get("vega_lite_chart") or app.get("arrow_vega_lite_chart")
        self.assertGreaterEqual(len(charts), 7)


if __name__ == "__main__":
    unittest.main()