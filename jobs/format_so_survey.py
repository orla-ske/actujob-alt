"""
jobs/format_so_survey.py
reads the raw stack overflow 2024 survey csv from s3,
extracts relevant columns, normalises them, and writes parquet back to s3.

raw:       raw/so_survey/2024/survey_results_public.csv
formatted: formatted/survey/2024/so_survey.parquet
"""
from __future__ import annotations

import io

import pandas as pd

from jobs.utils import s3 as s3_utils

RAW_KEY = "raw/so_survey/2024/survey_results_public.csv"
OUT_KEY = "formatted/survey/2024/so_survey.parquet"

# columns we care about from the ~80-column survey
KEEP_COLUMNS = [
    "ResponseId",
    "Employment",           # full-time, part-time, freelance
    "RemoteWork",           # remote, hybrid, in-person
    "DevType",              # developer roles (semicolon-separated)
    "YearsCodePro",         # years of professional coding experience
    "Country",
    "Currency",
    "CompTotal",            # self-reported total compensation
    "ConvertedCompYearly",  # normalised to usd by stack overflow
    "LanguageHaveWorkedWith",   # tech skills (semicolon-separated)
    "WebframeHaveWorkedWith",
    "DatabaseHaveWorkedWith",
    "PlatformHaveWorkedWith",
    "AISearchHaveWorkedWith",
]

# normalise the remotework field to our three-value vocabulary
REMOTE_WORK_MAP = {
    "Remote":    "remote",
    "Hybrid":    "hybrid",
    "In-person": "on-site",
}


def _parse_years(value: str | None) -> float | None:
    """convert 'less than 1 year', '50 or more years', '5' → float."""
    if pd.isna(value) or value is None:
        return None
    val = str(value).strip()
    if val == "Less than 1 year":
        return 0.5
    if "more" in val.lower():
        return 50.0
    try:
        return float(val)
    except ValueError:
        return None


def format_so_survey(**kwargs) -> None:
    """
    main callable for the airflow pythonoperator.
    not date-partitioned — the survey is a static 2024 file.
    """
    s3 = s3_utils.get_client()

    if s3_utils.key_exists(s3, OUT_KEY):
        print(f"[SKIP] {OUT_KEY} already exists")
        return

    print("Reading SO survey CSV from S3 ...")
    raw_bytes = s3_utils.get_bytes(s3, RAW_KEY)
    df = pd.read_csv(io.BytesIO(raw_bytes), low_memory=False)
    print(f"Loaded {len(df)} survey responses, {len(df.columns)} columns")

    # keep only the columns we need (drop missing ones gracefully)
    available = [c for c in KEEP_COLUMNS if c in df.columns]
    df = df[available].copy()

    # ── normalise ──────────────────────────────────────────────────────────────
    df["work_mode"]         = df["RemoteWork"].map(REMOTE_WORK_MAP).fillna("unknown")
    df["years_experience"]  = df["YearsCodePro"].apply(_parse_years)
    df["salary_usd"]        = pd.to_numeric(df["ConvertedCompYearly"], errors="coerce")
    df["salary_eur"]        = (df["salary_usd"] * 0.92).round(2)  # approximate usd→eur

    # explode semicolon-separated devtype into a flat list string
    df["dev_types"]         = df["DevType"].fillna("")
    df["primary_dev_type"]  = df["DevType"].str.split(";").str[0].str.strip()

    # explode languages into a flat string (es-friendly)
    df["languages"]         = df["LanguageHaveWorkedWith"].fillna("")

    df = df.rename(columns={"ResponseId": "respondent_id", "Country": "country"})
    df["source"] = "stackoverflow_2024"

    # drop rows where we have no salary and no dev type — they add no analytical value
    df = df.dropna(subset=["salary_eur", "primary_dev_type"], how="all")

    s3_utils.put_parquet(s3, OUT_KEY, df)
    print(f"[OK] Wrote {len(df)} normalised survey rows → {OUT_KEY}")
