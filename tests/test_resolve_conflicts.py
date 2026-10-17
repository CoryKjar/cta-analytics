import unittest
from datetime import datetime

from unittest.mock import MagicMock

from src.storage.resolve_rebuild_conflicts import apply_resolution_batch, canonical_rows_match, resolve_unknowns


class UnknownResolutionTests(unittest.TestCase):
    def setUp(self):
        self.row = (
            datetime(2026, 10, 5, 13, 1, 40), 40100, 30021, 7, 3, 1, "5",
            datetime(2026, 10, 5, 13, 1, 26), datetime(2026, 10, 5, 13, 2, 26),
            True, False, 41.9, -87.6, 212, 1.0, "812",
        )

    def variant(self, **changes):
        from src.storage.restore_train_identity import COLUMNS
        row = list(self.row)
        for name, value in changes.items():
            row[COLUMNS.index(name)] = value
        return tuple(row)

    def test_heading_only_nulls_heading_and_preserves_flags_and_gps(self):
        rows, audit = resolve_unknowns([self.row, self.variant(heading=191)])
        self.assertEqual(rows, [self.variant(heading=None)])
        self.assertEqual(audit[0]["null_fields"], ["heading"])
        self.assertEqual(audit[0]["xml_entries"], [1, 2])
        self.assertEqual(len(audit[0]["normalized_variants"]), 2)

    def test_one_disputed_gps_coordinate_nulls_the_pair_only(self):
        rows, audit = resolve_unknowns([self.row, self.variant(latitude=42.0)])
        self.assertEqual(rows, [self.variant(latitude=None, longitude=None)])
        self.assertEqual(audit[0]["different_fields"], ["latitude"])
        self.assertEqual(audit[0]["null_fields"], ["latitude", "longitude"])

    def test_delay_conflict_is_unknown_not_true_or_false(self):
        rows, audit = resolve_unknowns([self.row, self.variant(is_delayed=True)])
        self.assertEqual(rows, [self.variant(is_delayed=None)])
        self.assertEqual(audit[0]["null_fields"], ["is_delayed"])

    def test_approaching_and_prediction_revisions_are_not_approved(self):
        for changed in (self.variant(is_approaching=False), self.variant(prediction_time=datetime(2026, 10, 5, 13))):
            with self.subTest(changed=changed), self.assertRaises(ValueError):
                resolve_unknowns([self.row, changed])

    def test_distinct_trains_and_singletons_remain_intact(self):
        second = self.variant(run_number="916", is_delayed=True)
        rows, audit = resolve_unknowns([self.row, second])
        self.assertEqual(rows, [self.row, second])
        self.assertEqual(audit, [])

    def test_identical_variants_deduplicate_without_nulls(self):
        rows, audit = resolve_unknowns([self.row, self.row])
        self.assertEqual(rows, [self.row])
        self.assertEqual(audit, [])

    def test_input_rows_are_not_modified(self):
        source = [list(self.row), list(self.variant(heading=191))]
        before = [row[:] for row in source]
        resolve_unknowns(source)
        self.assertEqual(source, before)

    def test_canonical_comparison_allows_real_precision_but_not_unknown_overwrite(self):
        expected = self.variant(heading=None)
        self.assertTrue(canonical_rows_match([self.variant(heading=None, latitude=41.900001)], [expected]))
        self.assertFalse(canonical_rows_match([self.row], [expected]))
        self.assertFalse(canonical_rows_match([self.variant(heading=None, is_delayed=True)], [expected]))

    def test_apply_refuses_concurrent_file_state_before_writing(self):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = []
        plan = {"record": (1, "source.xml", self.row[0], 1, "raw/source.xml", "etag")}
        with self.assertRaisesRegex(RuntimeError, "state changed"):
            apply_resolution_batch(conn, "cta_rebuild_test", [plan])
        conn.cursor.assert_not_called()


if __name__ == "__main__":
    unittest.main()