# Spark Migration Guide

This document explains what you need to swap the Pandas transformation tasks
for Apache Spark, earning the +1.5 bonus points on the rubric.
The migration is designed to be done **after** the Pandas pipeline is working.

---

## What changes — and what doesn't

| Layer | Pandas (now) | Spark (after migration) |
|---|---|---|
| Ingestion | `PythonOperator` + `requests` | Unchanged — Spark is not needed here |
| Formatting | `PythonOperator` + Pandas | `SparkSubmitOperator` + PySpark job |
| Combination | `PythonOperator` + Pandas | `SparkSubmitOperator` + PySpark job |
| Indexing | `PythonOperator` + ES client | Unchanged — Spark is not needed here |
| S3 utils | boto3 | Spark reads S3 natively via Hadoop |

The DAG structure does not change. Only the operator type for `format_*` and
`combine_kpis` tasks changes from `PythonOperator` to `SparkSubmitOperator`.

---

## Step 1 — Add Spark to Docker Compose

Add the following service to `docker-compose.yml`:

```yaml
  # ── Apache Spark (master + one worker) ────────────────────────────────────
  spark-master:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: master
      SPARK_RPC_AUTHENTICATION_ENABLED: no
      SPARK_RPC_ENCRYPTION_ENABLED: no
    ports:
      - "8081:8080"    # Spark master UI (use 8081 to avoid conflict with Airflow)
      - "7077:7077"    # Spark master port (used by SparkSubmitOperator)
    volumes:
      - ./jobs:/opt/bitnami/spark/jobs

  spark-worker:
    image: bitnami/spark:3.5
    environment:
      SPARK_MODE: worker
      SPARK_MASTER_URL: spark://spark-master:7077
      SPARK_WORKER_MEMORY: 2G    # adjust to your machine
      SPARK_WORKER_CORES: 2
      SPARK_RPC_AUTHENTICATION_ENABLED: no
      SPARK_RPC_ENCRYPTION_ENABLED: no
    depends_on:
      - spark-master
    volumes:
      - ./jobs:/opt/bitnami/spark/jobs
```

Also add the Spark connection to the Airflow section so the
`SparkSubmitOperator` knows where the master is:

```yaml
  # Inside x-airflow-common → environment:
  AIRFLOW_CONN_SPARK_DEFAULT: spark://spark-master:7077
```

---

## Step 2 — Add the Airflow Spark provider

Add to `requirements.txt`:

```
apache-airflow-providers-apache-spark==4.7.1
pyspark==3.5.1
```

Uncomment the `pyspark` line that is already in `requirements.txt`.

---

## Step 3 — Configure S3 access for Spark

Spark reads S3 through the Hadoop AWS connector. Add these JVM options to
the `SparkSubmitOperator` calls (shown in Step 5):

```python
conf = {
    "spark.hadoop.fs.s3a.endpoint":               "http://localstack:4566",
    "spark.hadoop.fs.s3a.access.key":             "test",
    "spark.hadoop.fs.s3a.secret.key":             "test",
    "spark.hadoop.fs.s3a.path.style.access":      "true",
    "spark.hadoop.fs.s3a.impl":                   "org.apache.hadoop.fs.s3a.S3AFileSystem",
    "spark.jars.packages": "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
}
```

Spark will now read `s3a://developer-job-market/...` paths directly,
bypassing boto3 entirely for the transformation jobs.

---

## Step 4 — Rewrite the formatting tasks as PySpark jobs

Create `jobs/spark_format_adzuna.py`:

```python
"""
PySpark version of format_adzuna.py
Run via: spark-submit jobs/spark_format_adzuna.py --date 2024-01-15
"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
import sys

DATE = sys.argv[1]  # passed by SparkSubmitOperator via application_args

spark = SparkSession.builder.appName("format_adzuna").getOrCreate()

# Read all raw JSON for this date directly from S3
raw_df = spark.read.json(f"s3a://developer-job-market/raw/adzuna_jobs/{DATE}/")

# Flatten and normalise — same logic as the Pandas version
formatted_df = (
    raw_df
    .select(
        F.col("results.id").alias("id"),
        F.col("results.title").alias("title"),
        F.col("results.salary_min").alias("salary_min"),
        F.col("results.salary_max").alias("salary_max"),
        F.col("results.location.display_name").alias("location"),
        F.col("results.contract_type").alias("contract_type"),
        F.col("results.created").alias("created"),
    )
    .withColumn("salary_avg", (F.col("salary_min") + F.col("salary_max")) / 2)
    .withColumn("created", F.to_timestamp("created"))
    .dropDuplicates(["id"])
)

# Write Parquet back to S3
(
    formatted_df
    .write
    .mode("overwrite")
    .parquet(f"s3a://developer-job-market/formatted/jobs/{DATE}/adzuna_spark.parquet")
)

print(f"Written {formatted_df.count()} rows")
spark.stop()
```

The combination job follows the same pattern — read two Parquet paths,
join with `df.join()`, aggregate with `groupBy().agg()`, write Parquet.

---

## Step 5 — Swap PythonOperator for SparkSubmitOperator in the DAG

```python
# Before (Pandas):
from airflow.operators.python import PythonOperator
from jobs.format_adzuna import format_adzuna_jobs

t_format_adzuna = PythonOperator(
    task_id="format_adzuna_jobs",
    python_callable=format_adzuna_jobs,
    op_kwargs={"ds": "{{ ds }}"},
)

# After (Spark):
from airflow.providers.apache.spark.operators.spark_submit import SparkSubmitOperator

t_format_adzuna = SparkSubmitOperator(
    task_id="format_adzuna_jobs",
    application="/opt/airflow/jobs/spark_format_adzuna.py",
    application_args=["{{ ds }}"],
    conn_id="spark_default",
    conf={
        "spark.hadoop.fs.s3a.endpoint":          "http://localstack:4566",
        "spark.hadoop.fs.s3a.access.key":        "test",
        "spark.hadoop.fs.s3a.secret.key":        "test",
        "spark.hadoop.fs.s3a.path.style.access": "true",
        "spark.hadoop.fs.s3a.impl":              "org.apache.hadoop.fs.s3a.S3AFileSystem",
        "spark.jars.packages":                   "org.apache.hadoop:hadoop-aws:3.3.4,com.amazonaws:aws-java-sdk-bundle:1.12.262",
    },
)
```

The dependency chain (`>>` operators) in the DAG does not change at all.

---

## Memory requirements

| Configuration | RAM needed |
|---|---|
| Current (Pandas only) | ~4 GB |
| + Spark master + 1 worker (2G) | ~8 GB |
| + Spark master + 2 workers (2G each) | ~12 GB |

Start with one worker. If your machine has less than 8 GB free, reduce
`SPARK_WORKER_MEMORY` to `1G` — the dataset is small enough that it works.

---

## Checklist before migrating

- [ ] Pandas pipeline runs end-to-end without errors
- [ ] All 4 KPI Parquets exist in S3
- [ ] Kibana dashboard is built and working
- [ ] Docker has at least 8 GB RAM allocated (Docker Desktop → Settings → Resources)
- [ ] `requirements.txt` updated with `pyspark` and Spark provider
- [ ] Spark services added to `docker-compose.yml`
- [ ] `AIRFLOW_CONN_SPARK_DEFAULT` added to Airflow environment

Migrate one task at a time (`format_adzuna` first), verify it in the Airflow
UI, then migrate `format_so_survey` and `combine_kpis`.
