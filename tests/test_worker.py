import signal
import threading
import unittest
from contextlib import suppress
from datetime import datetime, timezone
from unittest.mock import MagicMock, patch

from src.storage.collection_ledger import CollectionSkipped
from src.worker import next_interval, run_cycle, run_worker

BASE = datetime(2026, 10, 27, 12, 0, tzinfo=timezone.utc).timestamp()


class FakeStop:
    """Event stand-in whose wait advances a fake wall clock instead of sleeping."""

    def __init__(self, clock):
        self.clock = clock
        self.flag = False

    def is_set(self):
        return self.flag

    def set(self):
        self.flag = True

    def wait(self, timeout):
        self.clock[0] += timeout
        return self.flag


class NextIntervalTests(unittest.TestCase):
    def test_restart_early_in_interval_runs_immediately(self):
        self.assertEqual(next_interval(BASE + 30, None), (int(BASE // 300), BASE + 30))

    def test_restart_late_in_interval_waits_for_next_boundary(self):
        self.assertEqual(next_interval(BASE + 180, None), (int(BASE // 300) + 1, BASE + 300))

    def test_completed_interval_is_not_repeated(self):
        self.assertEqual(next_interval(BASE + 70, int(BASE // 300)), (int(BASE // 300) + 1, BASE + 300))

    def test_overrun_into_next_interval_starts_once_without_burst_catch_up(self):
        current = int(BASE // 300)
        self.assertEqual(next_interval(BASE + 300 + 40, current), (current + 1, BASE + 340))
        self.assertEqual(next_interval(BASE + 900 + 200, current), (current + 4, BASE + 1200))


class WorkerLoopTests(unittest.TestCase):
    def _worker(self, cycle, *, cycles=4, start=BASE + 10):
        clock = [start]
        stop = FakeStop(clock)
        starts = []

        def tracked(interval):
            starts.append(clock[0])
            cycle(interval, clock)
            if len(starts) >= cycles:
                stop.set()

        with patch("src.worker.logger"), patch("src.worker.load_dotenv"):
            run_worker(stop, clock=lambda: clock[0], cycle=tracked, startup_gap_hours=0)
        return starts

    def test_one_collection_per_interval_without_cumulative_drift(self):
        def slow(interval, clock):
            clock[0] += 95

        starts = self._worker(slow)
        self.assertEqual(starts, [BASE + 10, BASE + 300, BASE + 600, BASE + 900])

    def test_failures_and_database_outages_do_not_stop_the_loop(self):
        errors = [RuntimeError("private"), OSError("database down"), None, None]
        outcomes = []

        def flaky(interval, clock):
            error = errors.pop(0)
            with patch("src.worker._run_pipeline", side_effect=error), patch("src.worker.logger") as logger:
                outcomes.append(run_cycle(interval))
            self.assertNotIn("private", str(logger.mock_calls))

        self._worker(flaky)
        self.assertEqual(outcomes, ["failed", "failed", "completed", "completed"])

    def test_skipped_cycle_is_logged_as_skip_not_failure(self):
        with patch("src.worker._run_pipeline", side_effect=CollectionSkipped("interval_already_collected")), \
                patch("src.worker.logger") as logger:
            self.assertEqual(run_cycle(int(BASE // 300)), "skipped")
        logger.error.assert_not_called()
        self.assertIn("event=worker_cycle_finished", logger.info.call_args.args[0])

    def test_sigterm_finishes_current_collection_then_stops_and_restores_handler(self):
        previous = signal.getsignal(signal.SIGTERM)
        clock = [BASE + 5]
        stop = FakeStop(clock)
        completed = []

        def cycle(interval):
            signal.raise_signal(signal.SIGTERM)
            clock[0] += 60
            completed.append(interval)

        with patch("src.worker.logger") as logger, patch("src.worker.load_dotenv"):
            # Earlier Streamlit tests can leave a closed wakeup socket registered.
            wakeup_fd = signal.set_wakeup_fd(-1)
            try:
                run_worker(stop, clock=lambda: clock[0], cycle=cycle, startup_gap_hours=0)
            finally:
                with suppress(OSError, ValueError):
                    signal.set_wakeup_fd(wakeup_fd)
        self.assertEqual(len(completed), 1)
        self.assertTrue(stop.is_set())
        self.assertIs(signal.getsignal(signal.SIGTERM), previous)
        self.assertIn("event=worker_stopped", logger.info.call_args.args[0])

    def test_watchdog_exits_nonzero_only_when_a_cycle_hangs(self):
        for hangs, expected in ((True, [1]), (False, [])):
            with self.subTest(hangs=hangs):
                clock = [BASE + 10]
                stop = FakeStop(clock)
                released = threading.Event()
                exits = []

                def exit_process(code):
                    exits.append(code)
                    stop.set()
                    released.set()

                def cycle(interval):
                    if hangs:
                        released.wait(5)
                    else:
                        released.wait(0.15)
                        stop.set()

                with patch("src.worker.logger") as logger, patch("src.worker.load_dotenv"):
                    run_worker(
                        stop, clock=lambda: clock[0], cycle=cycle, startup_gap_hours=0,
                        cycle_timeout_seconds=0.05 if hangs else 1, exit_process=exit_process,
                    )
                self.assertEqual(exits, expected)
                if hangs:
                    self.assertIn("event=worker_cycle_timeout", logger.error.call_args.args[0])

    def test_restart_in_same_interval_relies_on_persisted_ledger_check(self):
        first = self._worker(lambda interval, clock: None, cycles=1)
        second = self._worker(lambda interval, clock: None, cycles=1, start=BASE + 20)
        # Both processes attempt the interval; the ledger's interval check makes the second skip.
        self.assertEqual((first, second), ([BASE + 10], [BASE + 20]))

    @patch("src.worker.psycopg.connect")
    @patch("src.worker.find_collection_gaps")
    def test_startup_reports_persisted_gaps_and_survives_database_outage(self, find, connect):
        find.return_value = [{
            "interval_start": datetime(2026, 10, 27, 11, 0, tzinfo=timezone.utc),
            "state": "missed", "cause": "not_started", "expected": None,
            "uploaded": 0, "awaiting_recovery": 0,
        }]
        clock = [BASE + 10]
        for failure in (None, OSError("database down")):
            with self.subTest(failure=failure):
                connect.side_effect = failure
                stop = FakeStop(clock)
                cycle = MagicMock(side_effect=lambda interval: stop.set())
                with patch.dict("os.environ", {"DATABASE_URL": "postgresql://unused"}), \
                        patch("src.worker.logger") as logger, patch("src.worker.load_dotenv"):
                    run_worker(stop, clock=lambda: clock[0], cycle=cycle, startup_gap_hours=6)
                cycle.assert_called_once()
                if failure is None:
                    self.assertIn("event=collection_gap_detected", logger.warning.call_args.args[0])
                    self.assertIn("read_only=on", connect.call_args.kwargs["options"])
                else:
                    self.assertIn("event=worker_gap_check_failed", logger.error.call_args.args[0])
                    self.assertNotIn("database down", str(logger.mock_calls))


if __name__ == "__main__":
    unittest.main()
