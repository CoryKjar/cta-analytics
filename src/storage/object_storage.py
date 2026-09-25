import os

import boto3
from botocore.config import Config
from dotenv import load_dotenv


def get_bucket_client():
    load_dotenv()

    endpoint = os.getenv("ENDPOINT")
    access_key = os.getenv("ACCESS_KEY_ID")
    secret_key = os.getenv("SECRET_ACCESS_KEY")
    region = os.getenv("REGION", "auto")

    print("Bucket configuration:")
    print(f"  ENDPOINT: {endpoint}")
    print(f"  ACCESS_KEY_ID present: {bool(access_key)}")
    print(f"  SECRET_ACCESS_KEY present: {bool(secret_key)}")
    print(f"  REGION: {region}")

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
            s3={
                "addressing_style": "virtual"
            }
        ),
    )


def upload_file(local_path, object_key):
    load_dotenv()

    bucket = os.getenv("BUCKET")
    print(f"  BUCKET: {bucket}")
    if not bucket:
        raise ValueError("BUCKET is not configured.")

    client = get_bucket_client()

    client.upload_file(
        str(local_path),
        bucket,
        object_key,
    )