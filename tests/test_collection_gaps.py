import io
import unittest
from contextlib import redirect_stdout
from datetime import datetime, timedelta, timezone
from unittest.mock import MagicMock, patch

from src.monitoring.collection_gaps import classify_intervals, group_episodes, main

T0 = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc)
STALE = timedelta(minutes=15)


def run(minute, *, seconds=20, duration=60, status="completed", uploaded=143, awaiting=0, deferred=0):
    started = T0 + timedelta(minutes=minute, seconds=seconds)
    completed = None if status == "running" else started + timedelta(seconds=duration)
    return (started, completed, status, 143, uploaded, awaiting, deferred)


def classify(runs, *, files=(), start=T0, end=None, now=None, ledger_start=T0):
    end = end or T0 + timedelta(minutes=30)
    return classify_intervals(
        runs, set(files), start=start, end=end, ledger_start=ledger_start,
        now=now or end, stale_after=STALE,
    )


class CollectionGapTests(unittest.TestCase):
    def test_normal_five_minute_collection_has_no_gaps(self):
        self.assertEqual(classify([run(minute) for minute in range(0, 30, 5)]), [])

    def test_missed_intervals_are_detected_from_persisted_runs_alone(self):
        # A fresh call has no in-memory state, as after a process restart.
        gaps = classify([run(0), run(5), run(20), run(25)])
        self.assertEqual([gap["interval_start"] for gap in gaps], [T0 + timedelta(minutes=10), T0 + timedelta(minutes=15)])
        self.assertEqual({(gap["state"], gap["cause"]) for gap in gaps}, {("missed", "not_started")})
        episode, = group_episodes(gaps)
        self.assertEqual(episode["intervals"], 2)

    def test_interval_skipped_while_previous_run_active_is_attributed_to_overlap(self):
        gaps = classify([run(0, duration=400), run(10), run(15), run(20), run(25)])
        self.assertEqual(len(gaps), 1)
        self.assertEqual((gaps[0]["state"], gaps[0]["cause"]), ("missed", "previous_run_still_active"))

    def test_successful_empty_collection_is_not_a_gap(self):
        # Valid zero-arrival responses are uploaded snapshots; the run is complete.
        runs = [run(minute) for minute in range(0, 30, 5)]
        self.assertEqual(classify(runs), [])

    def test_partial_failed_and_unloaded_runs_are_distinguished(self):
        runs = [
            run(0), run(5, status="partial", uploaded=138), run(10, status="failed", uploaded=0),
            run(15, awaiting=3), run(20), run(25),
        ]
        gaps = classify(runs)
        self.assertEqual(
            [(gap["state"], gap["uploaded"], gap["awaiting_recovery"]) for gap in gaps],
            [("partial", 138, 0), ("failed", 0, 0), ("awaiting_recovery", 143, 3)],
        )

    def test_retry_in_same_interval_counts_best_outcome(self):
        runs = [run(minute) for minute in range(0, 30, 5)]
        runs.insert(1, run(0, seconds=5, status="failed", uploaded=0, duration=10))
        self.assertEqual(classify(runs), [])

    def test_bucket_outage_with_held_xml_awaits_recovery_then_completes(self):
        held = run(5, status="failed", uploaded=0, awaiting=143, deferred=143)
        mixed = run(10, status="partial", uploaded=103, awaiting=40, deferred=40)
        lost = run(15, status="partial", uploaded=100, awaiting=20, deferred=20)
        runs = [run(0), held, mixed, lost, run(20), run(25)]
        self.assertEqual(
            [(gap["state"], gap["deferred"]) for gap in classify(runs)],
            [("awaiting_recovery", 143), ("awaiting_recovery", 40), ("partial", 20)],
        )
        recovered = [run(0), run(5, status="failed", uploaded=0, deferred=143), run(10), run(15), run(20), run(25)]
        self.assertEqual(classify(recovered), [])

    def test_ledger_outage_with_processed_files_is_unrecorded_not_missing(self):
        runs = [run(minute) for minute in range(0, 30, 5) if minute != 10]
        gaps = classify(runs, files={T0 + timedelta(minutes=10)})
        self.assertEqual([(gap["state"], gap["cause"]) for gap in gaps], [("unrecorded", "ledger_unavailable")])

    def test_stale_running_run_is_incomplete_and_does_not_mask_later_misses(self):
        runs = [run(0, status="running")]
        gaps = classify(runs, end=T0 + timedelta(minutes=30))
        self.assertEqual(gaps[0]["state"], "incomplete")
        causes = [gap["cause"] for gap in gaps[1:]]
        self.assertEqual(causes[:2], ["previous_run_still_active"] * 2)
        self.assertEqual(causes[-1], "not_started")

    def test_current_interval_and_pre_ledger_history_are_not_reported(self):
        runs = [run(10), run(15)]
        now = T0 + timedelta(minutes=21)
        gaps = classify(runs, start=T0 - timedelta(hours=1), end=now, now=now, ledger_start=runs[0][0])
        self.assertEqual(gaps, [])
        self.assertEqual(classify([], ledger_start=None), [])

    def test_utc_intervals_are_continuous_across_chicago_fall_back(self):
        start = datetime(2026, 11, 1, 6, 0, tzinfo=timezone.utc)
        runs = [(start + timedelta(minutes=m, seconds=30), start + timedelta(minutes=m, seconds=90),
                 "completed", 143, 143, 0, 0) for m in range(0, 120, 5)]
        gaps = classify_intervals(
            runs, set(), start=start, end=start + timedelta(hours=2), ledger_start=start,
            now=start + timedelta(hours=2), stale_after=STALE,
        )
        self.assertEqual(gaps, [])

    @patch("src.monitoring.collection_gaps.psycopg.connect")
    @patch("src.monitoring.collection_gaps.find_collection_gaps")
    def test_report_states_unrecoverable_gaps_honestly_and_reads_only(self, find, connect):
        find.return_value = [
            {"interval_start": T0, "state": "missed", "cause": "not_started",
             "expected": None, "uploaded": 0, "awaiting_recovery": 0},
            {"interval_start": T0 + timedelta(minutes=5), "state": "awaiting_recovery", "cause": None,
             "expected": 143, "uploaded": 143, "awaiting_recovery": 2},
        ]
        output = io.StringIO()
        with patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"}), \
                patch("src.monitoring.collection_gaps.load_dotenv"), redirect_stdout(output):
            main(["--hours", "2"])
        text = output.getvalue()
        self.assertIn("not recoverable (CTA API has no history)", text)
        self.assertIn("XML archived or held in pending_uploads; hourly recovery loads it", text)
        self.assertIn("default_transaction_read_only=on", connect.call_args.kwargs["options"])


class HealthGapIssueTests(unittest.TestCase):
    @patch("src.monitoring.health_check.STATIONS", {"Station": 1})
    @patch("src.monitoring.health_check.connect")
    def test_missed_interval_raises_health_issue(self, connect):
        now = datetime.now(timezone.utc)
        conn = connect.return_value.__enter__.return_value
        ledger_start = now - timedelta(days=2)
        recent = now - timedelta(minutes=40)

        def execute(query, parameters=None):
            cursor = MagicMock()
            cursor.fetchall.return_value = []
            cursor.fetchone.return_value = (0,)
            if "to_regclass('public.collection_runs')" in query:
                cursor.fetchone.return_value = ("collection_runs",)
            elif "gap_runs" in query:
                cursor.fetchall.return_value = [(recent, recent + timedelta(seconds=60), "completed", 143, 143, 0, 0, ledger_start)]
            elif "GROUP BY station_id" in query:
                cursor.fetchall.return_value = [(1, now)]
            elif "MAX(collection_timestamp" in query:
                cursor.fetchone.return_value = (now,)
            return cursor

        conn.execute.side_effect = execute
        from src.monitoring.health_check import collect_health
        health = collect_health()
        self.assertIn("collection_interval_missed", health["issues"])
        self.assertIn("never started", health["issues"]["collection_interval_missed"])
        self.assertTrue(all(gap["state"] == "missed" for gap in health["collection_gaps"]))


if __name__ == "__main__":
    unittest.main()
