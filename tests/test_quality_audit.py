import hashlib
import unittest
from unittest.mock import MagicMock

from psycopg import sql

from src.storage.quality_audit import write_quality_audit
from src.storage.resolve_rebuild_conflicts import POLICY


class QualityAuditReplayTests(unittest.TestCase):
    def test_changed_source_cannot_replace_existing_audit(self):
        cursor = MagicMock()
        cursor.fetchone.return_value = (POLICY, "different-digest", 1, {})
        with self.assertRaisesRegex(ValueError, "refusing to overwrite"):
            write_quality_audit(cursor, sql.Identifier("public", "arrival_resolution_audit"),
                                (1, "source.xml", None, 1, "raw/source.xml"), [(1,)], b"source", [])
        self.assertEqual(cursor.execute.call_count, 1)

    def test_identical_replay_preserves_original_audit(self):
        cursor = MagicMock()
        data = b"source"
        evidence = {"policy": POLICY, "source_file": "source.xml", "object_key": "raw/source.xml", "decisions": []}
        cursor.fetchone.return_value = (POLICY, hashlib.sha256(data).hexdigest(), 1, evidence)
        write_quality_audit(cursor, sql.Identifier("public", "arrival_resolution_audit"),
                            (1, "source.xml", None, 1, "raw/source.xml"), [(1,)], data, [])
        self.assertEqual(cursor.execute.call_count, 1)


if __name__ == "__main__":
    unittest.main()