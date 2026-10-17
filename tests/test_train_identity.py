import unittest
from datetime import datetime
from unittest.mock import MagicMock
import xml.etree.ElementTree as ET

from src.storage.migrate_train_identity import validate_run_index
from src.storage.restore_train_identity import source_records, validate_existing


class TrainIdentityRestoreTests(unittest.TestCase):
    def setUp(self):
        self.record = (177253, "20261005_130140_Morse.xml", datetime(2026, 10, 5, 13, 1, 40), 1)
        root = ET.Element("ctatt")
        ET.SubElement(root, "tmst").text = "20261005 13:01:40"
        for run, delayed in (("812", "1"), ("916", "0")):
            eta = ET.SubElement(root, "eta")
            values = {"staId": "40100", "staNm": "Morse", "stpId": "30021", "rt": "Red", "rn": run,
                      "destNm": "95th/Dan Ryan", "trDr": "5", "prdt": "20261005 13:01:26",
                      "arrT": "20261005 13:02:26", "isApp": "1", "isDly": delayed,
                      "lat": None, "lon": None, "heading": None}
            for name, value in values.items():
                ET.SubElement(eta, name).text = value
        self.xml = ET.tostring(root)
        self.rows = source_records(self.xml, self.record, {"Red": 7}, {"95th/Dan Ryan": 3})

    def test_original_collapsed_row_can_restore_two_distinct_runs(self):
        self.assertEqual([row[15] for row in self.rows], ["812", "916"])
        self.assertEqual([row[10] for row in self.rows], [True, False])
        legacy = (*self.rows[0][:15], None)
        validate_existing(self.record, [legacy], self.rows)

    def test_unknown_prediction_or_incorrect_row_count_refuses_restore(self):
        legacy = list(self.rows[0])
        legacy[15] = None
        legacy[2] = 999
        with self.assertRaises(ValueError):
            validate_existing(self.record, [tuple(legacy)], self.rows)
        with self.assertRaises(ValueError):
            validate_existing(self.record, [], self.rows)

    def test_wrong_source_timestamp_refuses_restore(self):
        wrong = (177253, self.record[1], datetime(2026, 10, 6), 1)
        with self.assertRaises(ValueError):
            source_records(self.xml, wrong, {"Red": 7}, {"95th/Dan Ryan": 3})

    def test_same_run_conflicting_flags_still_refuses_restore(self):
        root = ET.fromstring(self.xml)
        root.findall("eta")[1].find("rn").text = "812"
        with self.assertRaises(ValueError):
            source_records(ET.tostring(root), self.record, {"Red": 7}, {"95th/Dan Ryan": 3})

    def test_index_guard_rejects_missing_or_invalid_uniqueness(self):
        conn = MagicMock()
        for value in (None, (False, True, True, []), (True, True, False, [])):
            conn.execute.return_value.fetchone.return_value = value
            with self.assertRaises(RuntimeError):
                validate_run_index(conn)


if __name__ == "__main__":
    unittest.main()