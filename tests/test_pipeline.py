import os
import runpy
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from botocore.exceptions import ClientError

from src.pipeline import main
from src.ingestion.fetch_arrivals import fetch_arrivals, fetch_cta_response
from src.transform.parse_arrivals import parse_arrivals
from src.storage.object_storage import storage_error_details
from src.storage.self_heal import run_self_heal


class PipelineImportTests(unittest.TestCase):
    def test_collector_and_recovery_do_not_import_operator_commands(self):
        result = subprocess.run(
            [sys.executable, "-B", "-c",
             "import sys; import src.pipeline; import src.storage.self_heal; "
             "commands = [name for name in sys.modules if name.startswith('src.scripts.')]; "
             "assert not commands, commands"],
            cwd=Path(__file__).resolve().parents[1],
            capture_output=True, text=True, timeout=30,
        )
        self.assertEqual(result.returncode, 0, result.stderr)


class FetchResponseValidationTests(unittest.TestCase):
    @patch("src.ingestion.fetch_arrivals.time.sleep")
    @patch("src.ingestion.fetch_arrivals.requests.get")
    @patch("src.ingestion.fetch_arrivals.CollectionAttemptLedger")
    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file")
    def test_collector_records_retry_reason_and_final_success_separately(self, upload, logger, ledger_class, get, sleep):
        success = MagicMock(status_code=200)
        success.text = "<ctatt><tmst>20261019 19:55:00</tmst><errCd>0</errCd></ctatt>"
        get.side_effect = [MagicMock(status_code=503, headers={}), success]
        ledger = ledger_class.return_value
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"CTA_API_KEY": "private-key"}), \
                    patch("src.ingestion.fetch_arrivals.load_dotenv"), \
                    patch("src.ingestion.fetch_arrivals.STATIONS", {"Wilson": 40540}), \
                    patch("src.ingestion.fetch_arrivals.Path", return_value=Path(directory)):
                self.assertEqual(len(fetch_arrivals()), 1)
        ledger.record_retry_failure.assert_called_once_with(40540, http_status=503, error_type="HTTPError")
        ledger.record_request_attempt.assert_called_once_with(40540, 2)
        self.assertEqual(ledger.station_uploaded.call_args.kwargs["http_status"], 200)
        self.assertEqual(ledger.station_uploaded.call_args.kwargs["attempt_count"], 2)
        self.assertNotIn("private-key", str(logger.mock_calls) + str(ledger.mock_calls))

    @patch("src.ingestion.fetch_arrivals.time.sleep")
    @patch("src.ingestion.fetch_arrivals.requests.get")
    @patch("src.ingestion.fetch_arrivals.logger")
    def test_successful_request_preserves_safe_retry_context(self, logger, get, sleep):
        from requests.exceptions import Timeout
        for failure, expected in (
            (MagicMock(status_code=503, headers={}), {"http_status": 503, "error_type": "HTTPError"}),
            (Timeout("private request URL"), {"http_status": None, "error_type": "Timeout"}),
        ):
            with self.subTest(error_type=expected["error_type"]):
                success = MagicMock(status_code=200)
                get.side_effect = [failure, success]
                retry = MagicMock()
                result = fetch_cta_response("https://example.invalid", {}, "Wilson", on_retry=retry)
                self.assertIs(result, success)
                retry.assert_called_once_with(**expected)
                self.assertNotIn("private", str(retry.mock_calls) + str(logger.mock_calls))

    @patch("src.ingestion.fetch_arrivals.time.sleep")
    @patch("src.ingestion.fetch_arrivals.requests.get")
    @patch("src.ingestion.fetch_arrivals.logger")
    def test_request_retry_reports_attempt_numbers(self, logger, get, sleep):
        retry_response = MagicMock(status_code=503, headers={"Retry-After": "1"})
        success_response = MagicMock(status_code=200)
        get.side_effect = [retry_response, success_response]
        attempts = []

        result = fetch_cta_response(
            "https://example.invalid/arrivals", {"mapid": 40540}, "Wilson",
            run_id="run-123", on_attempt=attempts.append,
        )

        self.assertIs(result, success_response)
        self.assertEqual(attempts, [1, 2])
        self.assertIn("run_id=%s", logger.warning.call_args.args[0])
        sleep.assert_called_once_with(1.0)

    @patch("src.ingestion.fetch_arrivals.CollectionAttemptLedger")
    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file")
    @patch("src.ingestion.fetch_arrivals.fetch_cta_response")
    def test_api_errors_are_not_written_or_uploaded(self, fetch, upload, logger, ledger_class):
        ledger_class.return_value.run_id = "test-run"
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
                ledger_class.return_value.station_failed.assert_called()
                self.assertEqual(ledger_class.return_value.station_failed.call_args.kwargs["stage"], "response_validation")
        upload.assert_not_called()
        self.assertNotIn("sensitive-source-detail", str(logger.error.call_args_list))

    @patch("src.ingestion.fetch_arrivals.CollectionAttemptLedger")
    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file")
    @patch("src.ingestion.fetch_arrivals.fetch_cta_response")
    def test_successful_empty_response_is_uploaded_and_parseable(self, fetch, upload, logger, ledger_class):
        ledger_class.return_value.run_id = "test-run"
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
            ledger_class.return_value.station_uploaded.assert_called_once()
            ledger_class.return_value.finish.assert_called_once_with(
                uploaded_count=1, failed_count=0, fatal_error_type=None,
            )

    @patch("src.ingestion.fetch_arrivals.CollectionAttemptLedger")
    @patch("src.ingestion.fetch_arrivals.logger")
    @patch("src.ingestion.fetch_arrivals.upload_file", side_effect=RuntimeError("private storage detail"))
    @patch("src.ingestion.fetch_arrivals.fetch_cta_response")
    def test_upload_failure_records_safe_bucket_stage_and_object(self, fetch, upload, logger, ledger_class):
        ledger_class.return_value.run_id = "test-run"
        fetch.return_value.text = "<ctatt><tmst>20261016 16:21:19</tmst><errCd>0</errCd></ctatt>"
        fetch.return_value.status_code = 200
        with tempfile.TemporaryDirectory() as directory:
            with patch.dict(os.environ, {"CTA_API_KEY": "test-key"}), \
                    patch("src.ingestion.fetch_arrivals.load_dotenv"), \
                    patch("src.ingestion.fetch_arrivals.STATIONS", {"Wilson": 40540}), \
                    patch("src.ingestion.fetch_arrivals.Path", return_value=Path(directory)):
                self.assertEqual(fetch_arrivals(), [])

        failure = ledger_class.return_value.station_failed.call_args.kwargs
        self.assertEqual(failure["stage"], "bucket_upload")
        self.assertEqual(failure["http_status"], 200)
        self.assertEqual(failure["source_file"], "20261016_162119_Wilson.xml")
        self.assertEqual(failure["object_key"], "raw/2026/10/16/20261016_162119_Wilson.xml")
        self.assertNotIn("private storage detail", str(logger.error.call_args_list))


class RecoveryLoggingTests(unittest.TestCase):
    def setUp(self):
        self.name = "20261016_162119_Wilson.xml"
        self.directory = tempfile.TemporaryDirectory()
        self.addCleanup(self.directory.cleanup)
        self.mocks = {}
        for name in ("logger", "_clients", "_inventory", "_manifest", "_repair_counts", "parse_arrivals", "load_arrivals_data", "register_file_results", "cleanup_staged_files"):
            patcher = patch("src.storage.self_heal." + name)
            self.mocks[name] = patcher.start()
            self.addCleanup(patcher.stop)
        for name, value in (("DRY_RUN", False), ("STAGING_DIR", Path(self.directory.name))):
            patcher = patch("src.storage.self_heal." + name, value)
            patcher.start()
            self.addCleanup(patcher.stop)
        self.client = MagicMock()
        self.mocks["_clients"].return_value = (self.client, "bucket", "unused")
        self.mocks["_inventory"].return_value = {self.name: "raw/2026/10/16/" + self.name}
        self.mocks["_manifest"].return_value = ({}, [])
        self.mocks["_repair_counts"].return_value = []
        self.mocks["parse_arrivals"].return_value = pd.DataFrame()

    def test_successful_empty_recovery_logs_file_and_info_summary(self):
        result = run_self_heal()
        self.assertEqual(result["recovered_empty"], 1)
        logger = self.mocks["logger"]
        self.assertIn("event=recovery_completed", logger.info.call_args.args[0])
        self.assertTrue(any("event=recovery_file_completed" in call.args[0] for call in logger.info.call_args_list))
        logger.error.assert_not_called()
        self.mocks["cleanup_staged_files"].assert_called_once()

    def test_each_failure_stage_uses_error_without_logging_credentials(self):
        stages = ("download", "parse", "load", "empty_registration")
        for stage in stages:
            with self.subTest(stage=stage):
                for mock in self.mocks.values():
                    mock.reset_mock(side_effect=True)
                self.client.download_file.reset_mock(side_effect=True)
                self.mocks["parse_arrivals"].return_value = pd.DataFrame()
                error = RuntimeError("https://private:secret@host/signed?token=secret")
                if stage == "download":
                    self.client.download_file.side_effect = error
                elif stage == "parse":
                    self.mocks["parse_arrivals"].side_effect = error
                elif stage == "load":
                    self.mocks["parse_arrivals"].return_value = pd.DataFrame({"source_file": [self.name]})
                    self.mocks["load_arrivals_data"].side_effect = error
                else:
                    self.mocks["register_file_results"].side_effect = [error, None]
                result = run_self_heal()
                logger = self.mocks["logger"]
                self.assertIn("event=recovery_completed", logger.error.call_args.args[0])
                self.assertTrue(any("stage=" + stage in call.args[0] for call in logger.error.call_args_list))
                self.assertNotIn("secret", str(logger.mock_calls))
                self.assertNotIn("secret", str(self.mocks["register_file_results"].call_args_list))
                self.assertEqual(sum(result[key] for key in ("parse_failures", "download_failures", "load_failures")), 1)
                self.mocks["cleanup_staged_files"].assert_not_called()

    def test_dry_run_logs_candidates_without_downloads_or_writes(self):
        with patch("src.storage.self_heal.DRY_RUN", True):
            result = run_self_heal()
        self.assertTrue(result["dry_run"])
        self.client.download_file.assert_not_called()
        self.mocks["_repair_counts"].assert_not_called()
        self.mocks["register_file_results"].assert_not_called()
        self.assertIn("event=recovery_dry_run_completed", self.mocks["logger"].info.call_args.args[0])

    def test_inventory_failure_logs_only_error_type_and_still_raises(self):
        self.mocks["_inventory"].side_effect = RuntimeError("https://private:secret@host")
        with self.assertRaises(RuntimeError):
            run_self_heal()
        self.mocks["logger"].error.assert_called_once_with("event=recovery_failed error_type=%s", "RuntimeError")
        self.assertNotIn("secret", str(self.mocks["logger"].mock_calls))

    def test_retry_cap_is_logged_without_retrying_exhausted_files(self):
        self.mocks["_manifest"].return_value = ({self.name: {"status": "failed", "attempts": 100}}, [])
        result = run_self_heal()
        self.assertEqual(result["candidates"], 0)
        self.client.download_file.assert_not_called()
        self.assertIn("event=recovery_retry_cap_reached", self.mocks["logger"].warning.call_args.args[0])


class PipelineFailureTests(unittest.TestCase):
    @patch("src.pipeline.logger")
    @patch("src.pipeline.cleanup_staged_files")
    @patch("src.pipeline.register_file_results")
    @patch("src.pipeline.load_arrivals_data", side_effect=RuntimeError("private database URL"))
    @patch("src.pipeline.parse_arrivals")
    @patch("src.pipeline.fetch_arrivals")
    def test_load_failure_preserves_parse_and_load_issue_records(self, fetch, parse, load, register, archive, logger):
        fetch.return_value = ["20261019_195500_Wilson.xml", "20261019_195501_Loyola.xml"]
        parse.side_effect = [ValueError("private XML"), pd.DataFrame({"source_file": [fetch.return_value[1]]})]
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}):
            with self.assertRaises(RuntimeError):
                main()
        self.assertEqual(register.call_count, 2)
        records = [record for call in register.call_args_list for record in call.args[0]]
        self.assertEqual([record["last_error"] for record in records], ["parse: ValueError", "load: RuntimeError"])
        self.assertEqual([record["source_file"] for record in records], fetch.return_value)
        archive.assert_not_called()
        self.assertNotIn("private", str(register.mock_calls) + str(logger.mock_calls))

    @patch("src.pipeline.logger")
    @patch("src.pipeline.register_file_results", side_effect=ValueError("private registration error"))
    @patch("src.pipeline.load_arrivals_data", side_effect=RuntimeError("original load failure"))
    @patch("src.pipeline.parse_arrivals")
    @patch("src.pipeline.fetch_arrivals", return_value=["20261019_195500_Wilson.xml"])
    def test_failure_registration_does_not_mask_original_load_error(self, fetch, parse, load, register, logger):
        parse.return_value = pd.DataFrame({"source_file": fetch.return_value})
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}):
            with self.assertRaisesRegex(RuntimeError, "original load failure"):
                main()
        self.assertNotIn("private", str(logger.mock_calls))

    @patch("src.pipeline.logger")
    @patch("src.pipeline.cleanup_staged_files")
    @patch("src.pipeline.register_file_results", side_effect=[RuntimeError("private error"), None])
    @patch("src.pipeline.parse_arrivals", return_value=pd.DataFrame())
    @patch("src.pipeline.fetch_arrivals", return_value=["20261019_195500_Wilson.xml"])
    def test_empty_registration_failure_is_recorded(self, fetch, parse, register, archive, logger):
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}):
            with self.assertRaises(RuntimeError):
                main()
        self.assertEqual(register.call_args.args[0][0]["last_error"], "empty-file registration: RuntimeError")
        self.assertEqual(register.call_args.args[0][0]["status"], "failed")
        archive.assert_not_called()

    @patch("src.utils.logger.get_logger")
    @patch("src.ingestion.fetch_arrivals.fetch_arrivals", side_effect=RuntimeError("https://private:secret@host"))
    def test_command_failure_logs_safe_type_and_exits_nonzero(self, fetch, get_logger):
        with patch.dict(os.environ, {"SELF_HEAL_ENABLED": "false"}):
            with self.assertRaises(SystemExit) as raised:
                runpy.run_path(str(Path(__file__).resolve().parents[1] / "src" / "pipeline.py"), run_name="__main__")
        self.assertEqual(raised.exception.code, 1)
        get_logger.return_value.error.assert_called_once_with("event=pipeline_failed error_type=%s", "RuntimeError")
        self.assertNotIn("secret", str(get_logger.return_value.mock_calls))

    @patch("src.pipeline.logger")
    @patch("src.pipeline.cleanup_staged_files")
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
    @patch("src.pipeline.cleanup_staged_files")
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