import io
import logging
import tempfile
import unittest
from pathlib import Path
from logging.handlers import RotatingFileHandler
from unittest.mock import patch

from src.utils.logger import get_logger


class LoggerStreamTests(unittest.TestCase):
    def test_components_share_one_rotating_file_handler(self):
        output, errors = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "logs"
            with patch("src.utils.logger.Path", return_value=path), patch("sys.stdout", output), patch("sys.stderr", errors):
                first = get_logger("test_cta_first_component")
                second = get_logger("test_cta_second_component")
                try:
                    first_file = next(handler for handler in first.handlers if isinstance(handler, RotatingFileHandler))
                    second_file = next(handler for handler in second.handlers if isinstance(handler, RotatingFileHandler))
                    self.assertIs(first_file, second_file)
                    first_file.maxBytes = 256
                    for index in range(30):
                        component = first if index % 2 == 0 else second
                        component.info("shared_rotation=%02d payload=%s", index, "x" * 80)
                finally:
                    for logger in (first, second):
                        for handler in logger.handlers[:]:
                            handler.close()
                            logger.removeHandler(handler)
            files = list(path.glob("pipeline.log*"))
            self.assertEqual(len(files), 4)
            retained = "".join(item.read_text(encoding="utf-8") for item in files)
            self.assertIn("shared_rotation=28", retained)
            self.assertIn("shared_rotation=29", retained)
            self.assertEqual(errors.getvalue(), "")

    def test_file_rotation_is_bounded_and_keeps_latest_messages(self):
        output, errors = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "logs"
            with patch("src.utils.logger.Path", return_value=path), patch("sys.stdout", output), patch("sys.stderr", errors):
                logger = get_logger("test_cta_rotation")
                try:
                    handler = next(item for item in logger.handlers if isinstance(item, RotatingFileHandler))
                    self.assertEqual(handler.maxBytes, 5 * 1024 * 1024)
                    self.assertEqual(handler.backupCount, 3)
                    handler.maxBytes = 256
                    for index in range(30):
                        logger.info("rotation_probe=%02d payload=%s", index, "x" * 80)
                finally:
                    for handler in logger.handlers[:]:
                        handler.close()
                        logger.removeHandler(handler)
            files = list(path.glob("pipeline.log*"))
            self.assertEqual(len(files), 4)
            self.assertTrue((path / "pipeline.log.3").exists())
            self.assertIn("rotation_probe=29", (path / "pipeline.log").read_text(encoding="utf-8"))
            self.assertNotIn("rotation_probe=00", "".join(item.read_text(encoding="utf-8") for item in files))

    def test_normal_messages_use_stdout_and_errors_use_stderr_once(self):
        output, errors = io.StringIO(), io.StringIO()
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "logs"
            with patch("src.utils.logger.Path", return_value=path), patch("sys.stdout", output), patch("sys.stderr", errors):
                logger = get_logger("test_cta_streams")
                try:
                    logger.info("collector started")
                    logger.warning("retry requested")
                    logger.error("collector failed")
                    self.assertIs(get_logger("test_cta_streams"), logger)
                    self.assertEqual(len(logger.handlers), 3)
                finally:
                    for handler in logger.handlers[:]:
                        handler.close()
                        logger.removeHandler(handler)
            report = (path / "pipeline.log").read_text(encoding="utf-8")
        self.assertIn("collector started", output.getvalue())
        self.assertIn("retry requested", output.getvalue())
        self.assertNotIn("collector failed", output.getvalue())
        self.assertEqual(errors.getvalue().count("collector failed"), 1)
        self.assertNotIn("collector started", errors.getvalue())
        self.assertIn("Z | INFO", output.getvalue())
        for message in ("collector started", "retry requested", "collector failed"):
            self.assertEqual(report.count(message), 1)


if __name__ == "__main__":
    unittest.main()