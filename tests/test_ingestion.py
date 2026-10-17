import tempfile
import unittest
from datetime import datetime
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd

from src.config.stations import station_id_from_filename
from src.storage.load_arrivals_postgres import load_arrivals_data, register_file_results
from src.storage.repair_data_quality import (
    FLAG_REPAIR_SQL, STATION_REPAIR_SQL, apply_repairs,
    plan_boolean_repairs, plan_station_repairs, source_boolean_flags,
)
from src.transform.parse_arrivals import parse_arrivals, parse_cta_boolean


XML_SAMPLE = """<ctatt><tmst>20261016 16:21:19</tmst><eta>
<staId>40540</staId><staNm>Wilson</staNm><stpId>30105</stpId>
<rt>Red</rt><destNm>Howard</destNm><trDr>1</trDr>
<prdt>20261016 16:21:00</prdt><arrT>20261016 16:25:00</arrT>
<isApp>{approaching}</isApp><isDly>{delayed}</isDly>
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
        self.assertEqual(frame.loc[0, "latitude"], "41.9")
        self.assertEqual(frame.loc[0, "longitude"], "-87.6")

    def test_missing_and_zero_api_coordinates_are_not_fabricated(self):
        frame = sample_frame(latitude="", longitude="")
        self.assertIsNone(frame.loc[0, "latitude"])
        self.assertIsNone(frame.loc[0, "longitude"])
        frame = sample_frame(latitude="0.0", longitude="0.0")
        self.assertEqual(frame.loc[0, "latitude"], "0.0")
        self.assertEqual(frame.loc[0, "longitude"], "0.0")


class ArrivalLoaderTests(unittest.TestCase):
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
        frame = sample_frame()
        frame["is_approaching"] = "0"
        frame["is_delayed"] = "0"

        load_arrivals_data(frame)

        statement, records = cursor.executemany.call_args_list[-1].args
        self.assertIs(records[0][9], False)
        self.assertIs(records[0][10], False)
        self.assertIn("arrivals.file_id = EXCLUDED.file_id", statement)
        self.assertIn("IS DISTINCT FROM", statement)
        self.assertEqual(frame.loc[0, "is_approaching"], "0")

    @patch("src.storage.load_arrivals_postgres.psycopg.connect")
    def test_invalid_flag_fails_before_database_access(self, connect):
        frame = sample_frame()
        frame["is_delayed"] = "invalid"
        with self.assertRaises(ValueError):
            load_arrivals_data(frame)
        connect.assert_not_called()


class HistoricalRepairPlanTests(unittest.TestCase):
    def setUp(self):
        self.collected = datetime(2026, 10, 16, 16, 21, 19)
        self.record = (10, "20261016_162119_Wilson.xml", self.collected, 1)
        self.xml = XML_SAMPLE.format(approaching="0", delayed="0", latitude="", longitude="").encode()
        self.identity = (40540, 30105, 1, datetime(2026, 10, 16, 16, 21), datetime(2026, 10, 16, 16, 25))

    def test_station_backfill_requires_known_database_station(self):
        repairs, unresolved = plan_station_repairs([self.record], {40540})
        self.assertEqual(repairs, [(40540, 10)])
        self.assertEqual(unresolved, [])
        self.assertEqual(plan_station_repairs([self.record], set()), ([], [self.record[1]]))
        self.assertNotIn("updated_at", STATION_REPAIR_SQL)
        self.assertNotIn("processed_at", STATION_REPAIR_SQL)

    def test_source_backed_flag_repair_uses_exact_identity_and_old_values(self):
        flags = source_boolean_flags(self.xml, self.collected, {"Red": 1})
        repairs = plan_boolean_repairs(self.record, [(*self.identity, True, True)], flags)
        self.assertEqual(repairs, [(False, False, 10, self.collected, *self.identity, True, True)])
        self.assertEqual(plan_boolean_repairs(self.record, [(*self.identity, False, False)], flags), [])
        self.assertNotIn("processed_at", FLAG_REPAIR_SQL)
        self.assertIn("IS NOT DISTINCT FROM", FLAG_REPAIR_SQL)

    def test_wrong_source_timestamp_or_prediction_refuses_repair(self):
        with self.assertRaises(ValueError):
            source_boolean_flags(self.xml, datetime(2026, 10, 15), {"Red": 1})
        with self.assertRaises(ValueError):
            source_boolean_flags(self.xml, self.collected, {})
        with self.assertRaises(ValueError):
            plan_boolean_repairs(self.record, [(*self.identity, True, True)], {})
        with self.assertRaises(ValueError):
            plan_boolean_repairs(self.record, [], {})

    def test_apply_requires_every_planned_row_to_match(self):
        conn = MagicMock()
        cursor = conn.cursor.return_value.__enter__.return_value
        cursor.rowcount = 0
        with self.assertRaises(RuntimeError):
            apply_repairs(conn, STATION_REPAIR_SQL, [(40540, 10)])
        conn.transaction.return_value.__exit__.assert_called_once()


if __name__ == "__main__":
    unittest.main()