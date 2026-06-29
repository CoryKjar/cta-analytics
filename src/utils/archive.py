from pathlib import Path
import shutil


def archive_files(files, archive_folder="data/raw/arrivals/archive"):
    archive_path = Path(archive_folder)
    archive_path.mkdir(parents=True, exist_ok=True)

    for file in files:
        shutil.move(str(file), archive_path / file.name)

    print(f"Archived {len(files)} files.")