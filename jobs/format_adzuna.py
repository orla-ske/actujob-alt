"""
jobs/format_adzuna.py
Reads raw Adzuna JSON files from S3 (for a given date partition),
normalises them into a flat Parquet schema, and writes back to S3.

Raw:       raw/adzuna_jobs/{ds}/{country}_page_{n}.json
Formatted: formatted/jobs/{ds}/adzuna.parquet
"""
from __future__ import annotations

import json

import pandas as pd

from jobs.utils import s3 as s3_utils
from jobs.utils.skills import classify_work_mode, extract_skills

# ── Approximate EUR exchange rates (updated periodically) ──────────────────────
# For production: fetch live from https://data.ecb.europa.eu/
EUR_RATES: dict[str, float] = {
    "GBP": 1.17,
    "USD": 0.92,
    "EUR": 1.00,
    "CAD": 0.68,
    "AUD": 0.60,
    "CHF": 1.02,
}


def _to_eur(amount: float | None, currency: str) -> float | None:
    if amount is None:
        return None
    rate = EUR_RATES.get(currency.upper(), 1.0)
    return round(amount * rate, 2)


def _country_to_currency(country_code: str) -> str:
    mapping = {"gb": "GBP", "us": "USD", "fr": "EUR", "de": "EUR", "nl": "EUR"}
    return mapping.get(country_code.lower(), "EUR")


def _parse_posting(result: dict, country: str) -> dict:
    """Flatten a single Adzuna API result into our target schema."""
    currency = _country_to_currency(country)
    salary_min_raw = result.get("salary_min")
    salary_max_raw = result.get("salary_max")
    description = result.get("description", "") or ""

    return {
        "id":           result.get("id"),
        "title":        result.get("title"),
        "company":      result.get("company", {}).get("display_name"),
        "location":     result.get("location", {}).get("display_name"),
        "country":      country.upper(),
        "salary_min":   _to_eur(salary_min_raw, currency),
        "salary_max":   _to_eur(salary_max_raw, currency),
        "salary_avg":   (
            _to_eur((salary_min_raw + salary_max_raw) / 2, currency)
            if salary_min_raw and salary_max_raw else None
        ),
        "currency":     "EUR",
        "work_mode":    classify_work_mode(description, result.get("contract_type", "")),
        "contract_type":result.get("contract_type"),
        "skills":       extract_skills(description),          # list of strings
        "skills_str":   ",".join(extract_skills(description)),# ES-friendly flat string
        "created":      result.get("created"),
        "source":       "adzuna",
    }


def format_adzuna_jobs(ds: str, **kwargs) -> None:
    """
    Main callable for the Airflow PythonOperator.
    Reads all raw JSON files for `ds`, normalises, writes one Parquet file.
    """
    s3 = s3_utils.get_client()

    raw_prefix = f"raw/adzuna_jobs/{ds}/"
    out_key    = f"formatted/jobs/{ds}/adzuna.parquet"

    # Idempotency
    if s3_utils.key_exists(s3, out_key):
        print(f"[SKIP] {out_key} already exists")
        return

    raw_keys = s3_utils.list_keys(s3, raw_prefix)
    if not raw_keys:
        raise ValueError(f"No raw Adzuna files found under {raw_prefix}. Run ingestion first.")

    rows = []
    for key in raw_keys:
        # Extract country code from key: raw/adzuna_jobs/2024-01-01/gb_page_1.json
        filename = key.split("/")[-1]          # gb_page_1.json
        country  = filename.split("_")[0]       # gb

        raw_bytes = s3_utils.get_bytes(s3, key)
        payload   = json.loads(raw_bytes)

        for result in payload.get("results", []):
            rows.append(_parse_posting(result, country))

    if not rows:
        print("[WARN] No postings found in raw files — writing empty Parquet")

    df = pd.DataFrame(rows)
    df["created"] = pd.to_datetime(df["created"], utc=True, errors="coerce")
    df = df.drop_duplicates(subset=["id"])

    s3_utils.put_parquet(s3, out_key, df)
    print(f"[OK] Wrote {len(df)} normalised postings → {out_key}")
