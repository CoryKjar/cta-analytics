import unittest
from datetime import datetime
from unittest.mock import MagicMock, patch

from src.storage.rebuild_arrivals import acquire_worker_lock, cutover, read_prediction_file, rebuild_schema, rollback, validation_report
from src.storage.restore_train_identity import source_records


class StagedRebuildTests(unittest.TestCase):
    def test_schema_guard_never_accepts_live_schema_or_sql_fragments(self):
        self.assertEqual(rebuild_schema("cta_rebuild_pilot"), "cta_rebuild_pilot")
        for name in ("public", "arrivals", "cta_rebuild_", "cta_rebuild_a;drop schema public", "cta_rebuild_UPPER"):
            with self.subTest(name=name), self.assertRaises(ValueError):
                rebuild_schema(name)

    def test_empty_source_can_be_rebuilt_without_fake_arrivals(self):
        record = (1, "20261016_162119_Wilson.xml", datetime(2026, 10, 16, 16, 21, 19), 0)
        xml = b"<ctatt><tmst>20261016 16:21:19</tmst><errCd>0</errCd></ctatt>"
        self.assertEqual(source_records(xml, record, {}, {}, allow_empty=True), [])
        with self.assertRaises(ValueError):
            source_records(xml, record, {}, {})

    @patch("src.storage.rebuild_arrivals.source_records", return_value=[])
    def test_object_read_is_pinned_to_inventory_etag_and_closes_body(self, parse):
        client = MagicMock()
        body = MagicMock()
        body.read.return_value = b"source"
        client.get_object.return_value = {"Body": body}
        record = (1, "20261016_162119_Wilson.xml", datetime(2026, 10, 16, 16, 21, 19), 0, "raw/example.xml", '"etag"')
        rows, station, digest = read_prediction_file(client, "bucket", record, {}, {})
        self.assertEqual(rows, [])
        self.assertEqual(station, 40540)
        self.assertEqual(len(digest), 64)
        client.get_object.assert_called_once_with(Bucket="bucket", Key="raw/example.xml", IfMatch='"etag"')
        body.close.assert_called_once()

    def test_missing_source_inventory_refuses_download(self):
        client = MagicMock()
        record = (1, "20261016_162119_Wilson.xml", datetime(2026, 10, 16), 0, None, None)
        with self.assertRaises(ValueError):
            read_prediction_file(client, "bucket", record, {}, {})
        client.get_object.assert_not_called()

    @patch("src.storage.rebuild_arrivals.source_records")
    def test_source_station_must_match_filename(self, parse):
        parse.return_value = [(None, 123)]
        client = MagicMock()
        client.get_object.return_value["Body"].read.return_value = b"source"
        record = (1, "20261016_162119_Wilson.xml", datetime(2026, 10, 16), 1, "raw/source.xml", "etag")
        with self.assertRaisesRegex(ValueError, "station identity"):
            read_prediction_file(client, "bucket", record, {}, {})

    def test_worker_lock_blocks_concurrent_rebuild_operations(self):
        conn = MagicMock()
        conn.execute.return_value.fetchone.return_value = (False,)
        with self.assertRaises(RuntimeError):
            acquire_worker_lock(conn, "cta_rebuild_pilot")

    def test_cutover_requires_explicit_writer_pause_before_any_sql(self):
        conn = MagicMock()
        with self.assertRaisesRegex(ValueError, "Stop collector"):
            cutover(conn, "cta_rebuild_pilot", writers_paused=False)
        conn.execute.assert_not_called()

    @patch("src.storage.rebuild_arrivals.verify_database")
    def test_validation_queries_render_and_hash_regex_is_bound(self, verify):
        conn = MagicMock()
        def execute(statement, parameters=None):
            text = statement.as_string() if hasattr(statement, "as_string") else statement
            cursor = MagicMock()
            cursor.fetchall.return_value = [("loaded", 1)]
            if "run_number IS NULL" in text:
                cursor.fetchone.return_value = (2, 0, 0, 0)
            elif "SUM(row_count)" in text:
                cursor.fetchone.return_value = (2,)
            else:
                cursor.fetchone.return_value = (0,)
            return cursor
        conn.execute.side_effect = execute
        report = validation_report(conn, "cta_rebuild_test")
        self.assertTrue(report["ready_for_snapshot"])
        self.assertTrue(any(call.args[1:] == (("^[0-9a-f]{64}$",),) for call in conn.execute.call_args_list))

    def test_rollback_requires_paused_writers_before_any_sql(self):
        conn = MagicMock()
        with self.assertRaisesRegex(ValueError, "Pause all writers"):
            rollback(conn, "cta_rebuild_test", writers_paused=False)
        conn.execute.assert_not_called()


if __name__ == "__main__":
    unittest.main()