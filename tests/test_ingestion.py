import json
import os
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
    database_fingerprint, load_checkpoint, plan_boolean_batch, plan_boolean_repairs,
    plan_station_repairs, save_checkpoint, source_boolean_flags,
)
from src.storage.repair_data_quality import main as repair_main
from src.transform.parse_arrivals import (
    parse_arrivals, parse_arrivals_xml, parse_cta_boolean, parse_cta_run_number,
)


XML_SAMPLE = """<ctatt><tmst>20261016 16:21:19</tmst><eta>
<staId>40540</staId><staNm>Wilson</staNm><stpId>30105</stpId>
<rt>Red</rt><rn>812</rn><destNm>Howard</destNm><trDr>1</trDr>
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
        self.assertIsNone(rows[0][13])
        self.assertEqual(rows[0][11:13], (41.9, -87.6))
        audit.assert_called_once()
        self.assertEqual(audit.call_args.args[4], data)
        self.assertEqual(audit.call_args.args[5][0]["null_fields"], ["heading", "is_delayed"])

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
        self.assertIn("arrivals.file_id = EXCLUDED.file_id", statement)
        self.assertIn("IS DISTINCT FROM", statement)
        self.assertIn("arrival_time, run_number", statement)
        self.assertEqual(records[0][-1], "812")
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

    @patch("src.storage.repair_data_quality.read_source_flags")
    def test_parallel_batch_reads_database_once_and_matches_by_file(self, read_source):
        conn = MagicMock()
        second = (11, self.record[1], self.collected, 1)
        conn.execute.return_value.fetchall.return_value = [
            (11, *self.identity, True, True), (10, *self.identity, True, True),
        ]
        read_source.side_effect = lambda client, bucket, record, routes: {
            self.identity: (False, record[0] == 11),
        }
        repairs = plan_boolean_batch(conn, MagicMock(), "unused", [self.record, second], {"Red": 1}, 2)
        self.assertEqual(repairs, [
            (False, False, 10, self.collected, *self.identity, True, True),
            (False, True, 11, self.collected, *self.identity, True, True),
        ])
        conn.execute.assert_called_once()
        self.assertEqual(conn.execute.call_args.args[1], (self.collected, self.collected, [10, 11]))

    @patch("src.storage.repair_data_quality.read_source_flags", side_effect=ValueError("Bad source"))
    def test_source_failure_prevents_batch_plan(self, read_source):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [(10, *self.identity, True, True)]
        with self.assertRaisesRegex(ValueError, "Bad source"):
            plan_boolean_batch(conn, MagicMock(), "unused", [self.record], {"Red": 1}, 2)
        conn.transaction.assert_not_called()

    @patch("src.storage.repair_data_quality.read_source_flags")
    def test_explicit_skip_leaves_invalid_file_unchanged_and_reports_it(self, read_source):
        conn = MagicMock()
        conn.execute.return_value.fetchall.return_value = [
            (10, *self.identity, True, True), (11, *self.identity, True, True),
        ]
        def read(client, bucket, record, routes):
            if record[0] == 10:
                raise ValueError("Conflicting source flags")
            return {self.identity: (False, False)}
        read_source.side_effect = read
        invalid = []
        second = (11, self.record[1], self.collected, 1)
        repairs = plan_boolean_batch(conn, MagicMock(), "unused", [self.record, second], {"Red": 1}, 2, invalid)
        self.assertEqual(repairs, [(False, False, 11, self.collected, *self.identity, True, True)])
        self.assertEqual(invalid, [{"file_id": 10, "reason": "Conflicting source flags"}])

    def test_checkpoints_preserve_unresolved_files(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "flags.json"
            skipped = [{"file_id": 10, "reason": "Conflicting source flags"}]
            save_checkpoint(path, 20, 100, "test-db", skipped)
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["skipped"], skipped)

    def test_run_aware_flags_keep_same_time_trains_separate(self):
        flags = {(*self.identity, "812"): (True, True), (*self.identity, "916"): (True, False)}
        record = (*self.record[:3], 2)
        stored = [(*self.identity, True, True, "812"), (*self.identity, True, True, "916")]
        repairs = plan_boolean_repairs(record, stored, flags, include_runs=True)
        self.assertEqual(len(repairs), 1)
        self.assertEqual(repairs[0][-1], "916")
        self.assertEqual(repairs[0][:2], (True, False))
        with self.assertRaisesRegex(ValueError, "Legacy train identity"):
            plan_boolean_repairs(self.record, [(*self.identity, True, True, None)], flags, include_runs=True)

    def test_checkpoint_round_trip_and_database_guard(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "progress" / "flags.json"
            save_checkpoint(path, 10, 100, "test-database")
            self.assertEqual(load_checkpoint(path, "test-database"), (10, 100))
            self.assertFalse(path.with_name(path.name + ".tmp").exists())
            with self.assertRaises(ValueError):
                load_checkpoint(path, "different-database")
            save_checkpoint(path, 20, 100, "test-database")
            self.assertEqual(load_checkpoint(path, "test-database"), (20, 100))

    def test_checkpoint_rejects_impossible_bounds(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "flags.json"
            save_checkpoint(path, 200, 100, "test-database")
            with self.assertRaises(ValueError):
                load_checkpoint(path, "test-database")

    def test_database_identity_survives_endpoint_changes_but_rejects_other_clusters(self):
        first, second = MagicMock(), MagicMock()
        first.info.host, second.info.host = "localhost", "production-public-alias"
        first.info.port, second.info.port = 61063, 5432
        first.execute.return_value.fetchone.return_value = ("cluster-a", "railway", "16384")
        second.execute.return_value.fetchone.return_value = ("cluster-a", "railway", "16384")
        self.assertEqual(database_fingerprint(first), database_fingerprint(second))
        second.execute.return_value.fetchone.return_value = ("cluster-b", "railway", "16384")
        self.assertNotEqual(database_fingerprint(first), database_fingerprint(second))
        second.execute.return_value.fetchone.return_value = ("cluster-a", "another-db", "16400")
        self.assertNotEqual(database_fingerprint(first), database_fingerprint(second))

    def test_legacy_checkpoint_is_not_silently_rebound(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "legacy.json"
            path.write_text(json.dumps({"version": 1, "kind": "booleans", "database": "old-endpoint", "after": 10, "cutoff": 100}), encoding="utf-8")
            with self.assertRaisesRegex(ValueError, "Legacy checkpoint endpoint differs"):
                load_checkpoint(path, "stable-db", legacy_database="different-endpoint")
            self.assertEqual(load_checkpoint(path, "stable-db", legacy_database="old-endpoint"), (10, 100))
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["version"], 1)


class AutomaticRepairTests(unittest.TestCase):
    @patch("src.storage.repair_data_quality.load_dotenv")
    @patch("src.storage.repair_data_quality.get_bucket_client")
    @patch("src.storage.repair_data_quality.plan_boolean_batch", return_value=[("planned",)])
    @patch("src.storage.repair_data_quality.apply_repairs")
    @patch("src.storage.repair_data_quality.psycopg.connect")
    def test_all_and_resume_use_committed_checkpoint_and_fixed_cutoff(self, connect, apply, plan, client, load_env):
        conn = connect.return_value.__enter__.return_value
        conn.info.host, conn.info.port, conn.info.dbname = "test-host", 5432, "test-db"
        maximum = [100]
        records = [(file_id, "sample.xml", datetime(2026, 10, 16), 1) for file_id in (10, 11, 12, 150)]
        bounds = []

        def execute(statement, parameters=None):
            cursor = MagicMock()
            if "pg_control_system" in statement:
                cursor.fetchone.return_value = ("test-cluster", "test-db", "16384")
            elif "MAX(file_id)" in statement:
                cursor.fetchone.return_value = (maximum[0],)
            elif "SELECT file_id,source_file" in statement:
                after, cutoff, limit = parameters
                bounds.append(cutoff)
                cursor.fetchall.return_value = [record for record in records if after < record[0] <= cutoff][:limit]
            elif "SELECT COUNT(*)" in statement:
                after, cutoff = parameters
                cursor.fetchone.return_value = (sum(after < record[0] <= cutoff for record in records),)
            elif "SELECT EXISTS" in statement:
                cursor.fetchone.return_value = (True,)
            else:
                cursor.fetchall.return_value = [(1, "Red")]
            return cursor

        conn.execute.side_effect = execute
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"DATABASE_URL": "unused", "BUCKET": "unused"}):
            path = Path(directory) / "checkpoint.json"
            arguments = ["repair", "--kind", "booleans", "--all", "--apply", "--limit", "2", "--batch-size", "1", "--checkpoint", str(path)]
            with patch("sys.argv", arguments):
                repair_main()
            saved = json.loads(path.read_text(encoding="utf-8"))
            self.assertEqual((saved["after"], saved["cutoff"]), (12, 100))
            self.assertEqual(apply.call_count, 3)
            maximum[0] = 200
            with patch("sys.argv", ["repair", "--kind", "booleans", "--resume", "--apply", "--checkpoint", str(path)]):
                repair_main()
            self.assertEqual(apply.call_count, 3)
            self.assertTrue(all(cutoff == 100 for cutoff in bounds))

    @patch("src.storage.repair_data_quality.load_dotenv")
    @patch("src.storage.repair_data_quality.get_bucket_client")
    @patch("src.storage.repair_data_quality.plan_boolean_batch", return_value=[("planned",)])
    @patch("src.storage.repair_data_quality.apply_repairs", side_effect=[None, RuntimeError("rolled back")])
    @patch("src.storage.repair_data_quality.psycopg.connect")
    def test_failed_second_batch_does_not_advance_progress(self, connect, apply, plan, client, load_env):
        conn = connect.return_value.__enter__.return_value
        conn.info.host, conn.info.port, conn.info.dbname = "test-host", 5432, "test-db"
        cursor = conn.execute.return_value
        cursor.fetchone.side_effect = [("test-cluster", "test-db", "16384"), (100,), (True,), (2,)]
        cursor.fetchall.side_effect = [
            [(10, "one.xml", datetime(2026, 10, 16), 1), (11, "two.xml", datetime(2026, 10, 16), 1)],
            [(1, "Red")],
        ]
        with tempfile.TemporaryDirectory() as directory, patch.dict(os.environ, {"DATABASE_URL": "unused", "BUCKET": "unused"}):
            path = Path(directory) / "checkpoint.json"
            with patch("sys.argv", ["repair", "--kind", "booleans", "--all", "--apply", "--batch-size", "1", "--checkpoint", str(path)]):
                with self.assertRaisesRegex(RuntimeError, "rolled back"):
                    repair_main()
            self.assertEqual(json.loads(path.read_text(encoding="utf-8"))["after"], 10)


if __name__ == "__main__":
    unittest.main()