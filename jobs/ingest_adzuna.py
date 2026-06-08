"""
jobs/ingest_adzuna.py
Fetches developer job postings from the Adzuna API and stores raw JSON in S3.
Partitioned by logical execution date: raw/adzuna_jobs/{ds}/{country}_page_{n}.json
"""
from __future__ import annotations

import json
import os

import requests

from jobs.utils import s3 as s3_utils

COUNTRIES = ["gb", "us", "fr", "de", "nl"]
PAGES_PER_COUNTRY = 5        # 5 pages × 50 results = 250 postings per country
RESULTS_PER_PAGE = 50


def fetch_adzuna_jobs(ds: str, **kwargs) -> None:
    """
    Main callable for the Airflow PythonOperator.
    `ds` is injected by Airflow as the logical execution date (YYYY-MM-DD).
    """
    app_id  = os.environ["ADZUNA_APP_ID"]
    api_key = os.environ["ADZUNA_API_KEY"]
    s3      = s3_utils.get_client()

    s3_utils.ensure_bucket(s3)

    for country in COUNTRIES:
        for page in range(1, PAGES_PER_COUNTRY + 1):
            key = f"raw/adzuna_jobs/{ds}/{country}_page_{page}.json"

            # Idempotency: skip if already fetched for this date
            if s3_utils.key_exists(s3, key):
                print(f"[SKIP] {key} already exists")
                continue

            url = (
                f"https://api.adzuna.com/v1/api/jobs/{country}/search/{page}"
                f"?app_id={app_id}&app_key={api_key}"
                f"&results_per_page={RESULTS_PER_PAGE}"
                f"&what=developer+engineer+software"
                f"&content-type=application/json"
            )

            response = requests.get(url, timeout=30)
            response.raise_for_status()
            payload = response.json()

            s3_utils.put_json(s3, key, payload)
            print(f"[OK] Stored {len(payload.get('results', []))} postings → {key}")

    print(f"Adzuna ingestion complete for {ds}")
