"""
jobs/index_to_es.py
Reads the four KPI Parquet files from S3 and indexes them into Elasticsearch.
Each KPI table gets its own index. Uses bulk indexing for performance.

ES indices created:
  - dev-jobs-salary-by-skill
  - dev-jobs-skill-demand
  - dev-jobs-remote-ratio
  - dev-jobs-experience-salary
"""
from __future__ import annotations

import os

import pandas as pd
from elasticsearch import Elasticsearch, helpers

from jobs.utils import s3 as s3_utils

# Index names must be lowercase, no spaces
INDEX_MAP = {
    "salary_by_skill":          "dev-jobs-salary-by-skill",
    "skill_demand_ranking":     "dev-jobs-skill-demand",
    "remote_ratio_by_country":  "dev-jobs-remote-ratio",
    "experience_salary_matrix": "dev-jobs-experience-salary",
}


def _get_es_client() -> Elasticsearch:
    return Elasticsearch(
        hosts=[os.environ.get("ELASTICSEARCH_HOST", "http://elasticsearch:9200")],
        request_timeout=30,
    )


def _df_to_actions(df: pd.DataFrame, index: str, ds: str):
    """Generator yielding ES bulk action dicts from a DataFrame."""
    for _, row in df.iterrows():
        doc = row.where(pd.notna(row), None).to_dict()
        doc["pipeline_date"] = ds      # add execution date for time-series filtering in Kibana
        yield {"_index": index, "_source": doc}


def index_to_elasticsearch(ds: str, **kwargs) -> None:
    """
    Main callable for the Airflow PythonOperator.
    Reads all KPI Parquets for `ds` and bulk-indexes them into Elasticsearch.
    """
    s3 = s3_utils.get_client()
    es = _get_es_client()

    if not es.ping():
        raise ConnectionError("Cannot reach Elasticsearch — is the container running?")

    for kpi_name, index_name in INDEX_MAP.items():
        key = f"usage/kpis/{ds}/{kpi_name}.parquet"

        if not s3_utils.key_exists(s3, key):
            print(f"[WARN] {key} not found — skipping {kpi_name}")
            continue

        df = s3_utils.get_parquet(s3, key)

        # Replace NaN with None so ES doesn't receive NaN floats
        df = df.where(pd.notna(df), None)

        actions = list(_df_to_actions(df, index_name, ds))

        success, failed = helpers.bulk(es, actions, raise_on_error=False)
        print(f"[OK] {index_name}: {success} indexed, {len(failed)} failed")

    print(f"Elasticsearch indexing complete for {ds}")
