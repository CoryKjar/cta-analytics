import os
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from botocore.exceptions import ClientError

from src.pipeline import main
from src.storage.object_storage import storage_error_details


class PipelineFailureTests(unittest.TestCase):
    @patch("src.pipeline.logger")
    @patch("src.pipeline.archive_files")
    @patch("src.pipeline.register_file_results")
    @patch("src.pipeline.parse_arrivals", return_value=pd.DataFrame())
    @patch("src.pipeline.fetch_arrivals", return_value=[Path("20261016_162119_Wilson.xml")])
    def test_partial_collection_keeps_successes_but_fails_job(self, fetch, parse, register, archive, logger):
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}), patch("src.pipeline.STATIONS", {"Wilson": 40540, "Other": 123}):
            with self.assertRaisesRegex(RuntimeError, "uploaded 1 of 2"):
                main()
        register.assert_called_once()
        archive.assert_called_once()

    @patch("src.pipeline.logger")
    @patch("src.pipeline.load_arrivals_data")
    @patch("src.pipeline.fetch_arrivals", return_value=[])
    def test_total_collection_failure_is_not_a_successful_cron_run(self, fetch, load, logger):
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}):
            with self.assertRaisesRegex(RuntimeError, "No station snapshots"):
                main()
        load.assert_not_called()
        logger.error.assert_called_once()

    def test_storage_error_reports_safe_code_not_credentials_or_message(self):
        cause = ClientError({"Error": {"Code": "AccessDenied", "Message": "sensitive-url"},
                             "ResponseMetadata": {"HTTPStatusCode": 403}}, "Upload")
        wrapper = RuntimeError("credential-bearing-upload-url")
        wrapper.__cause__ = cause
        self.assertEqual(storage_error_details(wrapper), ("AccessDenied", 403))
        self.assertEqual(storage_error_details(ValueError("private")), ("unknown", None))

    def test_storage_error_rejects_unexpected_identifier_content(self):
        error = ClientError({"Error": {"Code": "https://credential-bearing-url"}}, "Upload")
        self.assertEqual(storage_error_details(error), ("unknown", None))


if __name__ == "__main__":
    unittest.main()