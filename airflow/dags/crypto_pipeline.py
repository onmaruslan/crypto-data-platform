from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import sys

sys.path.append("/opt/airflow/src")

from extract import fetch_crypto_data
from load import save_raw_to_s3
from transform import transform_raw_to_prices_df
from load_processed import save_processed_to_s3
from load_postgres import upsert_prices_to_postgres


def extract_task(ti):
    raw = fetch_crypto_data()
    save_raw_to_s3(raw)
    ti.xcom_push(key="raw", value=raw)


def transform_task(ti):
    raw = ti.xcom_pull(key="raw", task_ids="extract_raw")
    df = transform_raw_to_prices_df(raw)

    # Use the raw timestamp as a run identifier for the parquet filename
    run_ts = str(raw["timestamp"])
    save_processed_to_s3(df, run_ts)

    # Store a small payload in XCom (DataFrame is not stored in XCom)
    ti.xcom_push(key="run_ts", value=run_ts)


def load_to_postgres_task(ti):
    raw = ti.xcom_pull(key="raw", task_ids="extract_raw")
    df = transform_raw_to_prices_df(raw)
    upsert_prices_to_postgres(df)


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

    transform_and_save = PythonOperator(
        task_id="transform_and_save_parquet",
        python_callable=transform_task,
    )

    load_to_postgres = PythonOperator(
        task_id="load_to_postgres",
        python_callable=load_to_postgres_task,
    )

    extract_raw >> transform_and_save >> load_to_postgres