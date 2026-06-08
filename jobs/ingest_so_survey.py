"""
jobs/ingest_so_survey.py
Downloads the Stack Overflow 2024 Developer Survey CSV from GitHub
and stores it in S3. Idempotent: skips if already present.
"""
from __future__ import annotations

import requests

from jobs.utils import s3 as s3_utils

SURVEY_URL = (
    "https://media.githubusercontent.com/media/StackExchange/Survey/"
    "main/packages/archive/2024/results.csv"
)
S3_KEY = "raw/so_survey/2024/survey_results_public.csv"


def fetch_so_survey(**kwargs) -> None:
    """
    Main callable for the Airflow PythonOperator.
    Not date-partitioned — the 2024 survey is a static annual file.
    """
    s3 = s3_utils.get_client()
    s3_utils.ensure_bucket(s3)

    if s3_utils.key_exists(s3, S3_KEY):
        print(f"[SKIP] SO survey already in S3 at {S3_KEY}")
        return

    print(f"Downloading SO survey from {SURVEY_URL} ...")
    response = requests.get(SURVEY_URL, timeout=120, stream=True)
    response.raise_for_status()

    chunks = []
    total = 0
    for chunk in response.iter_content(chunk_size=8 * 1024 * 1024):
        chunks.append(chunk)
        total += len(chunk)
        print(f"  Downloaded {total / 1_000_000:.1f} MB ...")

    raw_bytes = b"".join(chunks)
    s3_utils.put_bytes(s3, S3_KEY, raw_bytes, content_type="text/csv")
    print(f"[OK] SO survey stored → {S3_KEY} ({total / 1_000_000:.1f} MB)")
