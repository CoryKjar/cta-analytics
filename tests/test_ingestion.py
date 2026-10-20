import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.config.stations import station_id_from_filename
from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results
from src.transform.parse_arrivals import (
    parse_arrivals, parse_arrivals_xml, parse_cta_boolean, parse_cta_run_number,
    source_prediction_records,
)


XML_SAMPLE = """<ctatt><tmst>20261016 16:21:19</tmst><eta>
<staId>40540</staId><staNm>Wilson</staNm><stpId>30105</stpId>
<rt>Red</rt><rn>812</rn><destNm>Howard</destNm><trDr>1</trDr>
<prdt>20261016 16:21:00</prdt><arrT>20261016 16:25:00</arrT>
<isApp>{approaching}</isApp><isDly>{delayed}</isDly><isSch>0</isSch><isFlt>0</isFlt>
<lat>{latitude}</lat><lon>{longitude}</lon><heading>0</heading>
</eta></ctatt>"""


def sample_frame(approaching="0", delayed="1", latitude="41.9", longitude="-87.6"):
    with tempfile.TemporaryDirectory() as directory:
        path = Path(directory) / "20261016_162119_Wilson.xml"
        path.write_text(XML_SAMPLE.format(
            approaching=approaching, delayed=delayed, latitude=latitude, longitude=longitude,
        ), encoding="utf-8")
        return parse_arrivals(path)


class BooleanParsingTests(unittest.TestCase):
    def test_binary_flags_and_missing_values(self):
        for value in ("0", " 0 ", 0, 0.0, False):
            with self.subTest(value=value):
                self.assertIs(parse_cta_boolean(value), False)
        for value in ("1", " 1 ", 1, 1.0, True):
            with self.subTest(value=value):
                self.assertIs(parse_cta_boolean(value), True)
        for value in (None, "", " ", pd.NA, float("nan")):
            with self.subTest(value=value):
                self.assertIsNone(parse_cta_boolean(value))

    def test_invalid_flags_are_not_silently_coerced(self):
        for value in ("false", "true", "2", 2, -1):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_cta_boolean(value)

    def test_xml_flags_are_boolean_and_coordinates_preserve_source_values(self):
        frame = sample_frame()
        self.assertFalse(frame.loc[0, "is_approaching"])
        self.assertTrue(frame.loc[0, "is_delayed"])
        self.assertFalse(frame.loc[0, "is_scheduled"])
        self.assertFalse(frame.loc[0, "is_fault"])
        self.assertEqual(frame.loc[0, "latitude"], "41.9")
        self.assertEqual(frame.loc[0, "longitude"], "-87.6")

    def test_schedule_and_fault_flags_preserve_true_values_and_missingness(self):
        xml = XML_SAMPLE.format(
            approaching="0", delayed="0", latitude="", longitude="",
        ).replace("<isSch>0</isSch>", "<isSch>1</isSch>")
        xml = xml.replace("<isFlt>0</isFlt>", "<isFlt>1</isFlt>")
        frame = parse_arrivals_xml(xml.encode(), "20261016_162119_Wilson.xml")
        self.assertTrue(frame.loc[0, "is_scheduled"])
        self.assertTrue(frame.loc[0, "is_fault"])

        xml = xml.replace("<isSch>1</isSch>", "").replace("<isFlt>1</isFlt>", "")
        frame = parse_arrivals_xml(xml.encode(), "20261016_162119_Wilson.xml")
        self.assertIsNone(frame.loc[0, "is_scheduled"])
        self.assertIsNone(frame.loc[0, "is_fault"])

    def test_missing_and_zero_api_coordinates_are_not_fabricated(self):
        frame = sample_frame(latitude="", longitude="")
        self.assertIsNone(frame.loc[0, "latitude"])
        self.assertIsNone(frame.loc[0, "longitude"])
        frame = sample_frame(latitude="0.0", longitude="0.0")
        self.assertEqual(frame.loc[0, "latitude"], "0.0")
        self.assertEqual(frame.loc[0, "longitude"], "0.0")

    def test_run_number_validation_preserves_identity(self):
        self.assertEqual(parse_cta_run_number("00812"), "00812")
        for value in (None, "", "unknown", True, 812.5):
            with self.subTest(value=value), self.assertRaises(ValueError):
                parse_cta_run_number(value)

    def test_distinct_runs_with_identical_arrival_times_remain_distinct(self):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(XML_SAMPLE.format(
            approaching="1", delayed="1", latitude="", longitude="",
        ))
        second = ET.fromstring(ET.tostring(root.find("eta")))
        second.find("rn").text = "916"
        second.find("isDly").text = "0"
        root.append(second)
        frame = parse_arrivals_xml(ET.tostring(root), "20261016_162119_Wilson.xml")
        self.assertEqual(frame["run_number"].tolist(), ["812", "916"])
        self.assertEqual(frame["is_delayed"].tolist(), [True, False])
        self.assertEqual(frame["arrival_time"].nunique(), 1)


class APIResponseValidationTests(unittest.TestCase):
    def test_api_errors_are_not_valid_empty_snapshots(self):
        for code in ("100", " 100 ", "", "invalid"):
            with self.subTest(code=code), self.assertRaisesRegex(ValueError, "API error"):
                parse_arrivals_xml(
                    f"<ctatt><tmst>20261016 16:21:19</tmst><errCd>{code}</errCd></ctatt>".encode(),
                    "20261016_162119_Wilson.xml",
                )

    def test_api_error_with_predictions_is_rejected_without_exposing_error_text(self):
        data = XML_SAMPLE.format(approaching="0", delayed="0", latitude="", longitude="")
        data = data.replace("<ctatt>", "<ctatt><errCd>100</errCd><errNm>sensitive-source-detail</errNm>")
        with self.assertRaisesRegex(ValueError, "API error") as raised:
            parse_arrivals_xml(data.encode(), "20261016_162119_Wilson.xml")
        self.assertNotIn("sensitive-source-detail", str(raised.exception))

    def test_successful_empty_snapshots_remain_valid(self):
        for error_element in ("", "<errCd>0</errCd>", "<errCd> 0 </errCd>"):
            with self.subTest(error_element=error_element):
                frame = parse_arrivals_xml(
                    f"<ctatt><tmst>20261016 16:21:19</tmst>{error_element}</ctatt>".encode(),
                    "20261016_162119_Wilson.xml",
                )
                self.assertTrue(frame.empty)


class SourceVerificationTests(unittest.TestCase):
    def setUp(self):
        self.collected = pd.Timestamp("2026-10-16 16:21:19").to_pydatetime()
        self.record = (10, "20261016_162119_Wilson.xml", self.collected, 1)
        self.data = XML_SAMPLE.format(
            approaching="0", delayed="1", latitude="41.9", longitude="-87.6",
        ).encode()

    def test_source_records_match_the_loader_column_contract(self):
        rows = source_prediction_records(self.data, self.record, {"Red": 1}, {"Howard": 2})
        self.assertEqual(rows, [(
            self.collected, 40540, 30105, 1, 2, 10, "1",
            pd.Timestamp("2026-10-16 16:21:00").to_pydatetime(),
            pd.Timestamp("2026-10-16 16:25:00").to_pydatetime(),
            False, True, False, False, 41.9, -87.6, 0, 4.0, "812",
        )])

    def test_source_metadata_and_unknown_dimensions_are_rejected(self):
        wrong = (*self.record[:2], pd.Timestamp("2026-10-15").to_pydatetime(), 1)
        with self.assertRaisesRegex(ValueError, "timestamp/error"):
            source_prediction_records(self.data, wrong, {"Red": 1}, {"Howard": 2})
        for routes, destinations in (({}, {"Howard": 2}), ({"Red": 1}, {})):
            with self.subTest(routes=routes, destinations=destinations), self.assertRaisesRegex(ValueError, "dimensions"):
                source_prediction_records(self.data, self.record, routes, destinations)


class ArrivalLoaderTests(unittest.TestCase):
    @patch("src.storage.load_arrivals_postgres.write_quality_audit")
    @patch("src.storage.load_arrivals_postgres._database_url", return_value="unused")
    @patch("src.storage.load_arrivals_postgres.psycopg.connect")
    def test_live_loader_uses_same_unknown_policy_with_original_evidence(self, connect, database_url, audit):
        import xml.etree.ElementTree as ET
        root = ET.fromstring(XML_SAMPLE.format(approaching="0", delayed="1", latitude="41.9", longitude="-87.6"))
        duplicate = ET.fromstring(ET.tostring(root.find("eta")))
        duplicate.find("heading").text = "191"
        duplicate.find("isDly").text = "0"
        duplicate.find("isSch").text = "1"
        duplicate.find("isFlt").text = "1"
        root.append(duplicate)
        data = ET.tostring(root)
        frame = parse_arrivals_xml(data, "20261016_162119_Wilson.xml")
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        cursor.fetchall.side_effect = [[(1, "Red")], [(1, "Howard")], [(10, "20261016_162119_Wilson.xml")]]
        cursor.fetchone.return_value = (False,)
        cursor.execute.return_value.fetchall.return_value = []
        load_arrivals_data(frame)
        rows = cursor.executemany.call_args.args[1]
        self.assertEqual(len(rows), 1)
        self.assertIs(rows[0][9], False)
        self.assertIsNone(rows[0][10])
        self.assertEqual(rows[0][11:13], (None, None))
        self.assertEqual(rows[0][13:15], (41.9, -87.6))
        self.assertIsNone(rows[0][15])
        audit.assert_called_once()
        self.assertEqual(audit.call_args.args[4], data)
        self.assertEqual(
            audit.call_args.args[5][0]["null_fields"],
            ["heading", "is_delayed", "is_fault", "is_scheduled"],
        )

    def test_filename_station_lookup_is_exact(self):
        self.assertEqual(station_id_from_filename("20261016_162119_Wilson.xml"), 40540)
        self.assertEqual(
            station_id_from_filename("20261016_162119_Western_Blue_Forest_Park.xml"), 40220,
        )
        self.assertIsNone(station_id_from_filename("20261016_162119_Unknown.xml"))
        self.assertIsNone(station_id_from_filename("Wilson.xml"))

    @patch("src.storage.load_arrivals_postgres._database_url", return_value="unused")
    @patch("src.storage.load_arrivals_postgres.psycopg.connect")
    def test_empty_registration_maps_filename_without_overriding_explicit_station(self, connect, database_url):
        cursor = connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value
        results = [
            {"source_file": "20261016_162119_Wilson.xml", "collection_timestamp": pd.Timestamp("2026-10-16 16:21:19"), "status": "processed", "station_id": None},
            {"source_file": "20261016_162119_Wilson.xml", "collection_timestamp": pd.Timestamp("2026-10-16 16:21:19"), "status": "processed", "station_id": 123},
        ]

        register_file_results(results)

        records = cursor.executemany.call_args.args[1]
        self.assertEqual(records[0][2], 40540)
        self.assertEqual(records[1][2], 123)
        self.assertIsNone(results[0]["station_id"])

    @patch("src.storage.load_arrivals_postgres._database_url", return_value="unused")
    @patch("src.storage.load_arrivals_postgres.psycopg.connect")
    def test_loader_preserves_false_and_repairs_only_same_source_conflicts(self, connect, database_url):
        cursor = MagicMock()
        connect.return_value.__enter__.return_value.cursor.return_value.__enter__.return_value = cursor
        cursor.fetchall.side_effect = [[(1, "Red")], [(1, "Howard")], [(10, "20261016_162119_Wilson.xml")]]
        cursor.fetchone.return_value = (False,)
        frame = sample_frame()
        frame["is_approaching"] = "0"
        frame["is_delayed"] = "0"

        load_arrivals_data(frame)

        statement, records = cursor.executemany.call_args_list[-1].args
        self.assertIs(records[0][9], False)
        self.assertIs(records[0][10], False)
        self.assertEqual(records[0][11:13], (False, False))
        self.assertIn("arrivals.file_id = EXCLUDED.file_id", statement)
        self.assertIn("IS DISTINCT FROM", statement)
        self.assertIn("arrival_time, run_number", statement)
        self.assertEqual(records[0][-1], "812")
        self.assertEqual(frame.loc[0, "is_approaching"], "0")

    @patch("src.storage.load_arrivals_postgres.psycopg.connect")
    def test_invalid_flag_fails_before_database_access(self, connect):
        for column in ("is_delayed", "is_scheduled", "is_fault"):
            with self.subTest(column=column):
                frame = sample_frame()
                frame[column] = "invalid"
                with self.assertRaises(ValueError):
                    load_arrivals_data(frame)
        connect.assert_not_called()


if __name__ == "__main__":
    unittest.main()