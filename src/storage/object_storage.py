import os
import re

import boto3
from botocore.config import Config
from dotenv import load_dotenv


def storage_error_details(error):
    """Extract only safe SDK identifiers, never signed URLs or exception messages."""
    seen = set()
    while error is not None and id(error) not in seen:
        seen.add(id(error))
        response = getattr(error, "response", None)
        if isinstance(response, dict):
            code = str(response.get("Error", {}).get("Code", "unknown"))
            code = code if re.fullmatch(r"[A-Za-z0-9_.-]{1,80}", code) else "unknown"
            status = response.get("ResponseMetadata", {}).get("HTTPStatusCode")
            return code, status if isinstance(status, int) else None
        error = error.__cause__ or error.__context__
    return "unknown", None


def get_bucket_client(*, max_pool_connections=10):
    load_dotenv()

    endpoint = os.getenv("ENDPOINT")
    access_key = os.getenv("ACCESS_KEY_ID")
    secret_key = os.getenv("SECRET_ACCESS_KEY")
    region = os.getenv("REGION", "auto")

    if not all([endpoint, access_key, secret_key]):
        raise ValueError(
            "Railway Bucket credentials are not configured."
        )

    return boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        region_name=region,
        config=Config(
            max_pool_connections=max_pool_connections,
            # botocore defaults (60s timeouts, legacy retries) let slow uploads overrun the next cron slot.
            connect_timeout=5,
            read_timeout=15,
            retries={"max_attempts": 3, "mode": "standard"},
            s3={
                "addressing_style": "virtual"
            }
        ),
    )


def upload_file(local_path, object_key):
    load_dotenv()

    bucket = os.getenv("BUCKET")

    if not bucket:
        raise ValueError("BUCKET is not configured.")

    client = get_bucket_client()

    client.upload_file(
        str(local_path),
        bucket,
        object_key,
    )