"""
jobs/utils/s3.py
Shared S3 client and helper functions pointing at LocalStack.
All tasks import from here — credentials are read from environment variables.
"""
from __future__ import annotations

import io
import json
import os

import boto3
import pandas as pd

BUCKET = "developer-job-market"


def get_client() -> boto3.client:
    return boto3.client(
        "s3",
        endpoint_url=os.environ["AWS_ENDPOINT_URL"],
        aws_access_key_id=os.environ["AWS_ACCESS_KEY_ID"],
        aws_secret_access_key=os.environ["AWS_SECRET_ACCESS_KEY"],
        region_name="us-east-1",
    )


def ensure_bucket(s3) -> None:
    """Create the bucket if it does not already exist."""
    existing = [b["Name"] for b in s3.list_buckets().get("Buckets", [])]
    if BUCKET not in existing:
        s3.create_bucket(Bucket=BUCKET)


def key_exists(s3, key: str) -> bool:
    """Return True if an S3 key exists (used for idempotency checks)."""
    try:
        s3.head_object(Bucket=BUCKET, Key=key)
        return True
    except s3.exceptions.ClientError:
        return False


def put_json(s3, key: str, data: dict | list) -> None:
    s3.put_object(
        Bucket=BUCKET,
        Key=key,
        Body=json.dumps(data, ensure_ascii=False),
        ContentType="application/json",
    )


def put_bytes(s3, key: str, data: bytes, content_type: str = "application/octet-stream") -> None:
    s3.put_object(Bucket=BUCKET, Key=key, Body=data, ContentType=content_type)


def get_bytes(s3, key: str) -> bytes:
    return s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()


def put_parquet(s3, key: str, df: pd.DataFrame) -> None:
    """Write a Pandas DataFrame as Parquet directly into S3."""
    buffer = io.BytesIO()
    df.to_parquet(buffer, index=False, engine="pyarrow")
    buffer.seek(0)
    s3.put_object(Bucket=BUCKET, Key=key, Body=buffer.getvalue())


def get_parquet(s3, key: str) -> pd.DataFrame:
    """Read a Parquet file from S3 into a Pandas DataFrame."""
    body = s3.get_object(Bucket=BUCKET, Key=key)["Body"].read()
    return pd.read_parquet(io.BytesIO(body), engine="pyarrow")


def list_keys(s3, prefix: str) -> list[str]:
    """List all S3 keys under a given prefix."""
    paginator = s3.get_paginator("list_objects_v2")
    keys = []
    for page in paginator.paginate(Bucket=BUCKET, Prefix=prefix):
        for obj in page.get("Contents", []):
            keys.append(obj["Key"])
    return keys
