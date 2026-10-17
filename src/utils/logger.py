"""Shared logging configuration with explicit UTC timestamps."""
import logging
from logging.handlers import RotatingFileHandler
import sys
import time
from pathlib import Path


class UTCFormatter(logging.Formatter):
    """Format timestamps in UTC so Railway and application times are comparable."""
    converter = time.gmtime


class BelowErrorFilter(logging.Filter):
    def filter(self, record):
        return record.levelno < logging.ERROR


def get_logger(name="cta_pipeline"):
    log_folder = Path("logs")
    log_folder.mkdir(parents=True, exist_ok=True)

    logger = logging.getLogger(name)
    if not logger.handlers:
        logger.setLevel(logging.INFO)
        logger.propagate = False
        formatter = UTCFormatter(
            fmt="%(asctime)sZ | %(levelname)s | %(name)s | %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

        filename = str((log_folder / "pipeline.log").resolve())
        file_handler = next(
            (handler for existing in logging.Logger.manager.loggerDict.values()
             if isinstance(existing, logging.Logger)
             for handler in existing.handlers
             if isinstance(handler, RotatingFileHandler) and handler.baseFilename == filename),
            None,
        )
        if file_handler is None:
            file_handler = RotatingFileHandler(
                filename, maxBytes=5 * 1024 * 1024,
                backupCount=3, encoding="utf-8",
            )
        console_handler = logging.StreamHandler(sys.stdout)
        console_handler.addFilter(BelowErrorFilter())
        error_handler = logging.StreamHandler(sys.stderr)
        error_handler.setLevel(logging.ERROR)
        file_handler.setFormatter(formatter)
        console_handler.setFormatter(formatter)
        error_handler.setFormatter(formatter)
        logger.addHandler(file_handler)
        logger.addHandler(console_handler)
        logger.addHandler(error_handler)
    return logger
