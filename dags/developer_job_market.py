"""
dags/developer_job_market.py
main airflow dag for the developer job market pipeline.

stages:
  1. ingestion   — fetch raw data from adzuna api + so survey
  2. formatting  — normalise raw files to parquet (one task per source)
  3. combination — join both sources, produce 4 kpi aggregations
  4. indexing    — push kpi parquets into elasticsearch

dependency graph:

  fetch_adzuna ──► format_adzuna ──┐
                                   ├──► combine_kpis ──► index_to_es
  fetch_so     ──► format_so     ──┘
"""
from __future__ import annotations

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── import task callables from the jobs/ package ───────────────────────────────
from jobs.ingest_adzuna    import fetch_adzuna_jobs
from jobs.ingest_so_survey import fetch_so_survey
from jobs.format_adzuna    import format_adzuna_jobs
from jobs.format_so_survey import format_so_survey
from jobs.combine_kpis     import combine_kpis
from jobs.index_to_es      import index_to_elasticsearch

# ── default task arguments ─────────────────────────────────────────────────────
default_args = {
    "owner":          "orlando",
    "retries":        2,
    "retry_delay":    timedelta(minutes=5),
    "email_on_retry": False,
}

# ── dag definition ─────────────────────────────────────────────────────────────
with DAG(
    dag_id="developer_job_market_pipeline",
    description="Ingest, transform and index developer job market data",
    start_date=datetime(2025, 1, 1),
    schedule="@daily",
    catchup=False,                  # don't backfill historical dates
    default_args=default_args,
    tags=["big-data", "job-market", "isep"],
) as dag:

    # ── stage 1: ingestion ─────────────────────────────────────────────────────
    t_fetch_adzuna = PythonOperator(
        task_id="fetch_adzuna_jobs",
        python_callable=fetch_adzuna_jobs,
        op_kwargs={"ds": "{{ ds }}"},   # airflow injects yyyy-mm-dd
    )

    t_fetch_so = PythonOperator(
        task_id="fetch_so_survey",
        python_callable=fetch_so_survey,
        # no ds — survey is a static 2024 file
    )

    # ── stage 2: formatting ────────────────────────────────────────────────────
    t_format_adzuna = PythonOperator(
        task_id="format_adzuna_jobs",
        python_callable=format_adzuna_jobs,
        op_kwargs={"ds": "{{ ds }}"},
    )

    t_format_so = PythonOperator(
        task_id="format_so_survey",
        python_callable=format_so_survey,
    )

    # ── stage 3: combination ──────────────────────────────────────────────────
    t_combine = PythonOperator(
        task_id="combine_kpis",
        python_callable=combine_kpis,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # ── stage 4: indexing ──────────────────────────────────────────────────────
    t_index_es = PythonOperator(
        task_id="index_to_elasticsearch",
        python_callable=index_to_elasticsearch,
        op_kwargs={"ds": "{{ ds }}"},
    )

    # ── dependency chain ───────────────────────────────────────────────────────
    # fetch tasks are independent — they run in parallel
    # format tasks each depend on their own fetch task
    # combine waits for both format tasks
    # indexing runs after combine

    t_fetch_adzuna >> t_format_adzuna
    t_fetch_so     >> t_format_so
    [t_format_adzuna, t_format_so] >> t_combine >> t_index_es
