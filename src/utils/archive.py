from pathlib import Path

def archive_files(files):
    deleted = 0

    for file in files:
        file = Path(file)
        if file.exists():
            file.unlink()
            deleted += 1

    print(f"Removed {deleted} local staging files.")