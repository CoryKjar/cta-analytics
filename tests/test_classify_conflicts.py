import unittest
from datetime import datetime
import xml.etree.ElementTree as ET

from src.storage.classify_rebuild_conflicts import classify_source, classification_summary


class ConflictClassificationTests(unittest.TestCase):
    def setUp(self):
        self.record = (1, "20261005_130140_Morse.xml", datetime(2026, 10, 5, 13, 1, 40), 1, "raw/source.xml", "etag")
        self.values = {"staId": "40100", "staNm": "Morse", "stpId": "30021", "rt": "Red", "rn": "812",
                       "destNm": "95th/Dan Ryan", "trDr": "5", "prdt": "20261005 13:01:26", "arrT": "20261005 13:02:26",
                       "isApp": "1", "isDly": "0", "lat": "41.9", "lon": "-87.6", "heading": "212"}

    def classify(self, change):
        root = ET.Element("ctatt")
        ET.SubElement(root, "tmst").text = "20261005 13:01:40"
        for values in (self.values, {**self.values, **change}):
            eta = ET.SubElement(root, "eta")
            for name, value in values.items():
                ET.SubElement(eta, name).text = value
        return classify_source(ET.tostring(root), self.record, {"Red": 7}, {"95th/Dan Ryan": 3})

    def test_heading_only_is_telemetry_with_original_values_and_positions(self):
        result = self.classify({"heading": "191"})
        self.assertEqual(result["primary_category"], "telemetry_only")
        group = result["groups"][0]
        self.assertEqual(group["xml_entries"], [1, 2])
        self.assertEqual(group["different_fields"], ["heading"])
        self.assertEqual([item["value"] for item in group["normalized_differences"]["heading"]], [212, 191])

    def test_equivalent_numeric_formats_are_not_false_conflicts(self):
        result = self.classify({"heading": "0212", "lat": "41.90000", "isDly": " 0 "})
        self.assertEqual(result["primary_category"], "exact_duplicate")
        self.assertEqual(result["groups"][0]["different_fields"], [])
        self.assertIn("heading", result["groups"][0]["raw_field_differences"])

    def test_flags_and_revision_differences_are_nonexclusive(self):
        result = self.classify({"isDly": "1", "prdt": "20261005 13:00:26", "heading": "191"})
        self.assertEqual(result["primary_category"], "flag_conflict")
        self.assertEqual(set(result["groups"][0]["categories"]), {"flag_conflict", "prediction_revision", "telemetry_difference"})

    def test_different_runs_do_not_collide(self):
        result = self.classify({"rn": "916", "isDly": "1"})
        self.assertEqual(result["primary_category"], "no_collision_reproduced")
        self.assertEqual(result["groups"], [])

    def test_summary_distinguishes_file_and_group_counts(self):
        telemetry = self.classify({"heading": "191"})
        flags = self.classify({"isDly": "1"})
        summary = classification_summary([telemetry, flags], "cta_rebuild_test")
        self.assertEqual(summary["files_examined"], 2)
        self.assertEqual(summary["conflicting_groups"], 2)
        self.assertEqual(summary["primary_file_categories"], {"telemetry_only": 1, "flag_conflict": 1})


if __name__ == "__main__":
    unittest.main()