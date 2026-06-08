## Developer Job Market Pipeline (ActuJob (Placeholder Name?))

This project is a small big-data pipeline that looks at the developer job market. It pulls job postings from the Adzuna API and combines them with the Stack Overflow 2024 Developer Survey, turns everything into clean tables, and pushes a handful of useful metrics into Elasticsearch so you can explore them in Kibana.

Everything runs locally with Docker. Airflow handles the orchestration, S3 is emulated with LocalStack, and the heavy lifting on the data is done with Pandas. There is also a guide for swapping the Pandas steps for Apache Spark later on, but you do not need Spark to run the pipeline.

## What it does

The pipeline runs as a single Airflow DAG with four stages:

1. Ingestion. It fetches developer and engineering job postings from Adzuna for five countries (UK, US, France, Germany, Netherlands) and downloads the Stack Overflow 2024 survey CSV. Both land in S3 as raw files.
2. Formatting. The raw Adzuna JSON and the survey CSV are normalised into flat Parquet tables. Salaries are converted to euros, work mode (remote, hybrid, on-site) is inferred, and tech skills are pulled out of the job descriptions with simple keyword matching.
3. Combination. The two formatted sources are turned into four KPI tables: salary by skill, skill demand ranking, remote ratio by country, and an experience versus salary matrix.
4. Indexing. Each KPI table is bulk loaded into its own Elasticsearch index, ready to chart in Kibana.

Every step is idempotent. If a file already exists in S3 for a given date, that step is skipped, so you can rerun the DAG without creating duplicates.

## How it is laid out

```
dags/        the airflow dag that wires the tasks together
jobs/        the actual task code (ingest, format, combine, index)
jobs/utils/  shared helpers for s3 access and skill extraction
docker-compose.yml   all the services (airflow, postgres, localstack, elasticsearch, kibana)
requirements.txt     python dependencies
spark_migration.md   optional guide for moving the transforms to spark
```

## What you need first

You need Docker and Docker Compose installed, with at least 4 GB of RAM free for Docker. You also need a free Adzuna API account so you can fetch job postings. Sign up at the Adzuna developer site and grab your app id and app key.

## How to run

### Set up your environment file

The services read their configuration from a `.env` file in the project root. Create one with your own values:

```
ADZUNA_APP_ID=your_app_id
ADZUNA_API_KEY=your_app_key

AIRFLOW_UID=50000
_AIRFLOW_WWW_USER_USERNAME=admin
_AIRFLOW_WWW_USER_PASSWORD=admin

AWS_ACCESS_KEY_ID=test
AWS_SECRET_ACCESS_KEY=test
AWS_DEFAULT_REGION=us-east-1
S3_ENDPOINT_URL=http://localstack:4566
ELASTICSEARCH_HOST=http://elasticsearch:9200
```

The AWS values can stay as `test` because LocalStack does not check them. Only the two Adzuna values need to be real.

### Start the stack

```
docker compose up -d
```

The first start takes a few minutes while images download and Airflow sets up its database. Wait until the containers are healthy. You can watch progress with:

```
docker compose ps
```

### Open the interfaces

Once everything is up, these are available in your browser:

- Airflow at http://localhost:8080 (log in with the username and password from your `.env`)
- Kibana at http://localhost:5601
- Elasticsearch at http://localhost:9200

### Run the pipeline

In the Airflow UI, find the DAG named `developer_job_market_pipeline`. It starts paused, so turn it on with the toggle, then trigger a run with the play button. Watch the tasks go green one by one. The fetch tasks run first and in parallel, then formatting, then the combine step, and finally the indexing step.

When the run finishes, your KPI data is sitting in Elasticsearch and you can build charts in Kibana.

## How to test

There is no automated test suite in this repo, so testing here means checking that each stage produced what it should. The steps below walk through that from the outside in.

### Check the DAG loads cleanly

Before running anything, make sure Airflow parsed the DAG without import errors:

```
docker compose exec airflow-scheduler airflow dags list
```

You should see `developer_job_market_pipeline` in the list. If it is missing, check the scheduler logs for an import error.

### Run a single task at a time

You can run one task in isolation and read its output, which is the quickest way to find where something breaks. Pick any date for the run:

```
docker compose exec airflow-scheduler \
  airflow tasks test developer_job_market_pipeline fetch_adzuna_jobs 2025-01-01
```

Swap in the other task ids to test them one by one: `fetch_so_survey`, `format_adzuna_jobs`, `format_so_survey`, `combine_kpis`, and `index_to_elasticsearch`. Each task prints what it stored and where.

### Confirm the files landed in S3

Point the AWS CLI at LocalStack and list the bucket to confirm the raw, formatted, and KPI files exist:

```
aws --endpoint-url http://localhost:4566 s3 ls s3://developer-job-market/ --recursive
```

You should see raw files under `raw/`, Parquet files under `formatted/`, and the four KPI files under `usage/kpis/`.

### Confirm the data reached Elasticsearch

List the indices and check the document counts are not zero:

```
curl http://localhost:9200/_cat/indices/dev-jobs-*?v
```

You should see four indices: `dev-jobs-salary-by-skill`, `dev-jobs-skill-demand`, `dev-jobs-remote-ratio`, and `dev-jobs-experience-salary`. To peek at the actual documents in one of them:

```
curl http://localhost:9200/dev-jobs-salary-by-skill/_search?pretty
```

### Check it in Kibana

Open Kibana, create a data view that matches `dev-jobs-*`, and you should be able to browse the documents and build charts on top of them. If the documents are there, the whole pipeline worked end to end.

## Shutting down

To stop the services but keep your data:

```
docker compose down
```

To stop and wipe everything, including the S3 and Elasticsearch volumes, so you can start completely fresh:

```
docker compose down -v
```

## Moving to Spark later

If you want to swap the Pandas formatting and combination steps for Apache Spark, the full walkthrough is in `spark_migration.md`. The DAG shape stays the same. Only the operator type for those tasks changes, and you add a Spark master and worker to the Docker Compose file.
