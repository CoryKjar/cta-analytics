from pathlib import Path

from src.storage.object_storage import upload_file


test_file = Path("test_bucket.txt")

test_file.write_text(
    "CTA pipeline storage test",
    encoding="utf-8"
)

upload_file(
    test_file,
    "test/test_bucket.txt"
)

print("Bucket upload successful.")