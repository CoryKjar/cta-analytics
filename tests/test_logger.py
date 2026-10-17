import io
import logging
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from src.utils.logger import get_logger


class LoggerStreamTests(unittest.TestCase):
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