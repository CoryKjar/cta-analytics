import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

import pandas as pd
from botocore.exceptions import ClientError

from src.pipeline import main
from src.ingestion.fetch_arrivals import fetch_arrivals
from src.transform.parse_arrivals import parse_arrivals
from src.storage.object_storage import storage_error_details


class FetchResponseValidationTests(unittest.TestCase):
    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file")
    @patch("src.ingestion.fetch_arrivals.fetch_cta_response")
    def test_api_errors_are_not_written_or_uploaded(self, fetch, upload, logger):
        for code in ("100", "", "invalid"):
            fetch.return_value.text = (
                f"<ctatt><tmst>20261016 16:21:19</tmst><errCd>{code}</errCd>"
                "<errNm>sensitive-source-detail</errNm></ctatt>"
            )
            with self.subTest(code=code), tempfile.TemporaryDirectory() as directory:
                with patch.dict(os.environ, {"CTA_API_KEY": "test-key"}), \
                        patch("src.ingestion.fetch_arrivals.load_dotenv"), \
                        patch("src.ingestion.fetch_arrivals.STATIONS", {"Wilson": 40540}), \
                        patch("src.ingestion.fetch_arrivals.Path", return_value=Path(directory)):
                    self.assertEqual(fetch_arrivals(), [])
                self.assertEqual(list(Path(directory).iterdir()), [])
        upload.assert_not_called()
        self.assertNotIn("sensitive-source-detail", str(logger.error.call_args_list))

    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file")
    @patch("src.ingestion.fetch_arrivals.fetch_cta_response")
    def test_successful_empty_response_is_uploaded_and_parseable(self, fetch, upload, logger):
        fetch.return_value.text = "<ctatt><tmst>20261016 16:21:19</tmst><errCd>0</errCd></ctatt>"
        fetch.return_value.status_code = 200
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"CTA_API_KEY": "test-key"}), \
                    patch("src.ingestion.fetch_arrivals.load_dotenv"), \
                    patch("src.ingestion.fetch_arrivals.STATIONS", {"Wilson": 40540}), \
                    patch("src.ingestion.fetch_arrivals.Path", return_value=Path(directory)):
                uploaded = fetch_arrivals()
            expected = Path(directory) / "20261016_162119_Wilson.xml"
            self.assertEqual(uploaded, [expected])
            self.assertTrue(parse_arrivals(expected).empty)
            upload.assert_called_once_with(expected, "raw/2026/10/16/20261016_162119_Wilson.xml")


class PipelineFailureTests(unittest.TestCase):
    @patch("src.pipeline.logger")
    @patch("src.pipeline.archive_files")
    @patch("src.pipeline.register_file_results")
    @patch("src.pipeline.load_arrivals_data")
    def test_api_error_source_is_failed_not_processed_or_archived(self, load, register, archive, logger):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "20261016_162119_Wilson.xml"
            path.write_text("<ctatt><tmst>20261016 16:21:19</tmst><errCd>100</errCd></ctatt>", encoding="utf-8")
            with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}), \
                    patch("src.pipeline.STATIONS", {"Wilson": 40540}), \
                    patch("src.pipeline.fetch_arrivals", return_value=[path]):
                with self.assertRaisesRegex(RuntimeError, "failed source files"):
                    main()
            self.assertEqual(register.call_args.args[0][0]["status"], "failed")
            self.assertTrue(path.exists())
        load.assert_not_called()
        archive.assert_not_called()

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