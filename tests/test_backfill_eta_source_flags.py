import unittest
from datetime import datetime
from unittest.mock import MagicMock

from src.storage.backfill_eta_source_flags import _manifest_batch, _plan_updates


class SourceFlagBackfillPlanTests(unittest.TestCase):
    def setUp(self):
        self.collected = datetime(2026, 10, 19, 8, 0)
        self.arrival = datetime(2026, 10, 19, 8, 4)
        self.source_row = (
            self.collected, 40100, 30021, 2, 3, 7, "5",
            datetime(2026, 10, 19, 8, 0), self.arrival,
            False, False, True, False, 41.9, -87.6, 212, 4.0, "812",
        )
        self.prepared = {
            "file_id": 7,
            "source_hash": "a" * 64,
            "source_count": 1,
            "rows": [self.source_row],
            "decision_count": 0,
        }
        self.connection = MagicMock()
        cursor = self.connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = [(
            7, self.collected, 40100, 30021, 2, self.arrival, "812", None, None,
        )]

    def test_manifest_query_filters_to_requested_shard(self):
        cursor = self.connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        _manifest_batch(
            self.connection, None, None, None, 25, shard_count=4, shard_index=2,
        )

        query, params = cursor.execute.call_args.args
        self.assertIn("mod(f.file_id, %s) = %s", query)
        self.assertEqual(params, [4, 2, 25])

    def test_plan_updates_only_source_flags_for_exact_match(self):
        staged, ledger, updates, errors = _plan_updates(
            self.connection, [self.prepared], apply=False,
        )

        self.assertEqual(staged, [(
            7, self.collected, 30021, 2, self.arrival, "812", True, False,
        )])
        self.assertEqual(ledger[0][0], 7)
        self.assertEqual(ledger[0][4:7], (1, 1, 1))
        self.assertEqual(updates, 1)
        self.assertEqual(errors, [])

    def test_unmatched_source_rows_are_skipped_not_guessed(self):
        cursor = self.connection.cursor.return_value.__enter__.return_value
        cursor.fetchall.return_value = []

        staged, ledger, updates, errors = _plan_updates(
            self.connection, [self.prepared], apply=False,
        )

        self.assertEqual(staged, [])
        self.assertEqual(ledger, [])
        self.assertEqual(updates, 0)
        self.assertEqual(errors, [(7, "ValueError")])

    def test_apply_plan_keeps_row_locks_until_the_update_transaction(self):
        _plan_updates(self.connection, [self.prepared], apply=True)

        self.connection.cursor.return_value.__enter__.return_value.execute.assert_called_once()
        self.assertIn("FOR UPDATE", self.connection.cursor.return_value.__enter__.return_value.execute.call_args.args[0])
        self.connection.commit.assert_not_called()


if __name__ == "__main__":
    unittest.main()