from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

# Allow imports from /opt/airflow/src
sys.path.append("/opt/airflow/src")

# ingestion
from ingestion.extract_api import fetch_crypto_data

# storage layers
from storage.s3_raw import save_raw_to_s3, read_raw_from_s3
from storage.s3_staging import save_parquet_to_s3, read_parquet_from_s3
from storage.postgres_loader import upsert_prices

# processing
from processing.transform import transform_raw_to_prices_df


def extract_task(ti):
    """
    1. Fetch data from API
    2. Save to RAW S3
    3. Pass only S3 key via XCom
    """

    raw = fetch_crypto_data()

    raw_key = save_raw_to_s3(raw)

    ti.xcom_push(key="raw_key", value=raw_key)


def transform_task(ti):
    """
    1. Read RAW from S3
    2. Transform → DataFrame
    3. Save to STAGING (Parquet)
    4. Pass staging key via XCom
    """

    raw_key = ti.xcom_pull(key="raw_key", task_ids="extract_raw")

    raw = read_raw_from_s3(raw_key)

    df = transform_raw_to_prices_df(raw)

    run_ts = str(raw["timestamp"])

    staging_key = save_parquet_to_s3(df, run_ts)

    ti.xcom_push(key="staging_key", value=staging_key)


def load_task(ti):
    """
    1. Read Parquet from STAGING
    2. Load into Postgres (idempotent upsert)
    """

    staging_key = ti.xcom_pull(key="staging_key", task_ids="transform")

    df = read_parquet_from_s3(staging_key)

    upsert_prices(df)


with DAG(
    dag_id="crypto_pipeline",
    start_date=datetime(2024, 1, 1),
    schedule="@hourly",
    catchup=False,
) as dag:

    extract_raw = PythonOperator(
        task_id="extract_raw",
        python_callable=extract_task,
    )

    transform = PythonOperator(
        task_id="transform",
        python_callable=transform_task,
    )

    load = PythonOperator(
        task_id="load",
        python_callable=load_task,
    )

    extract_raw >> transform >> load