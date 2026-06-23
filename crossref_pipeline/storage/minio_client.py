import logging
from pathlib import Path

import boto3
from botocore.client import Config

from crossref_pipeline.utils.logging_utils import log_stage

logger = logging.getLogger(__name__)

MINIO_ENDPOINT = "http://localhost:9000"
MINIO_ACCESS_KEY = "minioadmin"
MINIO_SECRET_KEY = "minioadmin"
RAW_BUCKET = "crossref-raw"


def get_minio_client():
    return boto3.client(
        "s3",
        endpoint_url=MINIO_ENDPOINT,
        aws_access_key_id=MINIO_ACCESS_KEY,
        aws_secret_access_key=MINIO_SECRET_KEY,
        config=Config(signature_version="s3v4"),
    )


@log_stage
def ensure_bucket_exists(bucket_name: str = RAW_BUCKET) -> None:
    client = get_minio_client()

    existing_buckets = [
        bucket["Name"]
        for bucket in client.list_buckets().get("Buckets", [])
    ]

    if bucket_name not in existing_buckets:
        client.create_bucket(Bucket=bucket_name)
        logger.info("Created MinIO bucket: %s", bucket_name)


@log_stage
def upload_raw_file_to_minio(file_path: Path, bucket_name: str = RAW_BUCKET) -> str:
    ensure_bucket_exists(bucket_name)

    client = get_minio_client()
    object_key = f"crossref/{file_path.name}"

    client.upload_file(
        Filename=str(file_path),
        Bucket=bucket_name,
        Key=object_key,
    )

    s3_uri = f"s3://{bucket_name}/{object_key}"
    logger.info("Uploaded raw file to MinIO: %s", s3_uri)

    return s3_uri