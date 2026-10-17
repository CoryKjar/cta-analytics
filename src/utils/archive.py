from pathlib import Path
from src.utils.logger import get_logger


logger = get_logger(__name__)

def archive_files(files):
    deleted = 0

    for file in files:
        file = Path(file)
        if file.exists():
            file.unlink()
            deleted += 1

    logger.info("event=local_staging_cleaned files=%s", deleted)