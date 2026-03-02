"""
Sales Data Pipeline DAG
=======================
Flow: MinIO (CSV files) → Clean & Validate → PostgreSQL → Refresh Views

Schedule: Every hour
"""

import io
import logging
import os
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.python import PythonOperator
from airflow.utils.dates import days_ago
from minio import Minio
from minio.error import S3Error

from airflow import DAG

log = logging.getLogger(__name__)


# Static constant — not read from env, so safe at module level
ARCHIVE_BUCKET = "archive"


def _get_minio_config() -> dict:
    """
    Resolve MinIO settings at task-execution time, not at DAG-parse time.
    Airflow's scheduler parses DAG files continuously; any os.getenv() at
    module level is evaluated in the scheduler process and may not see the
    env vars injected into worker/task processes.  Calling this inside each
    task callable ensures the live environment is always used.
    """
    return {
        "endpoint": os.getenv("MINIO_ENDPOINT", "minio:9000"),
        "access_key": os.getenv("MINIO_ACCESS_KEY", "minioadmin"),
        "secret_key": os.getenv("MINIO_SECRET_KEY", "minioadmin123"),
        "bucket": os.getenv("MINIO_BUCKET", "sales-data"),
    }


def _get_db_config() -> dict:
    """
    Resolve PostgreSQL settings at task-execution time, not at DAG-parse time.
    Same rationale as _get_minio_config().
    """
    return {
        "host": os.getenv("DATA_DB_HOST", "postgres"),
        "port": int(os.getenv("DATA_DB_PORT", "5432")),
        "dbname": os.getenv("DATA_DB_NAME", "salesdb"),
        "user": os.getenv("DATA_DB_USER", "datauser"),
        "password": os.getenv("DATA_DB_PASSWORD", "datapassword"),
    }


DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": ["richard.sarfo@amalitech.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}


EXPECTED_COLUMNS = [
    "transaction_id",
    "transaction_date",
    "customer_id",
    "customer_name",
    "customer_email",
    "customer_region",
    "product_id",
    "product_name",
    "product_category",
    "quantity",
    "unit_price",
    "total_amount",
    "discount",
    "payment_method",
    "order_status",
]


def get_minio_client() -> Minio:
    """Return a MinIO client using runtime environment variables."""
    cfg = _get_minio_config()
    return Minio(
        cfg["endpoint"],
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        secure=False,
    )


def get_db_connection():
    """Return a psycopg2 connection using runtime environment variables."""
    return psycopg2.connect(**_get_db_config())


def check_for_new_files(**context):
    """Scan MinIO 'incoming/' prefix for CSV files to process."""
    client = get_minio_client()
    source_bucket = _get_minio_config()["bucket"]  # resolved at runtime
    try:
        objects = list(client.list_objects(source_bucket, prefix="incoming/", recursive=True))
    except S3Error as e:
        log.warning(f"MinIO error: {e}")
        objects = []

    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]

    if not csv_files:
        log.info("No new CSV files found in MinIO.")
    else:
        log.info(f"Found {len(csv_files)} file(s): {csv_files}")

    context["ti"].xcom_push(key="files_to_process", value=csv_files)
    log.info(f"MinIO endpoint in use: {_get_minio_config()['endpoint']}")
    return csv_files


def process_and_load_files(**context):
    """Download, clean, validate, and load each CSV file into PostgreSQL."""
    ti = context["ti"]
    files = ti.xcom_pull(key="files_to_process", task_ids="check_for_new_files")

    if not files:
        log.info("No files to process.")
        return {"processed": 0, "total_records": 0}

    log.info(
        "DB config in use: host=%s dbname=%s user=%s",
        _get_db_config()["host"],
        _get_db_config()["dbname"],
        _get_db_config()["user"],
    )
    client = get_minio_client()  # one client reused across files is fine

    total_inserted = 0
    total_failed = 0
    files_processed = 0

    for object_name in files:
        log.info(f"Processing: {object_name}")
        # Open a fresh connection per file so that a rollback on one file
        # does not leave the connection in a broken state for subsequent files.
        conn = get_db_connection()
        try:
            # Download from MinIO (bucket resolved at runtime, not parse time)
            response = client.get_object(_get_minio_config()["bucket"], object_name)
            csv_data = response.read().decode("utf-8")
            response.close()

            # load into dataframe
            df = pd.read_csv(io.BytesIO(csv_data.encode("utf-8")))

            # Validation
            missing_cols = [c for c in EXPECTED_COLUMNS if c not in df.columns]
            if missing_cols:
                log.warning(f"Missing columns in {object_name}: {missing_cols}")
                continue  # Skip processing this file if missing expected columns
            original_count = len(df)

            # Data Cleaning

            df = df.dropna(subset=["transaction_id", "transaction_date"])
            df = df.drop_duplicates(subset=["transaction_id"])
            df["transaction_date"] = pd.to_datetime(df["transaction_date"], errors="coerce").dt.date
            df = df.dropna(subset=["transaction_date"])
            df["quantity"] = pd.to_numeric(df["quantity"], errors="coerce").fillna(1).astype(int)
            df["unit_price"] = pd.to_numeric(df["unit_price"], errors="coerce").fillna(0)
            df["total_amount"] = pd.to_numeric(df["total_amount"], errors="coerce").fillna(0)
            df["discount"] = pd.to_numeric(df["discount"], errors="coerce").fillna(0)
            df["customer_name"] = df["customer_name"].str.strip().str.title()
            df["product_category"] = df["product_category"].str.strip().str.title()
            df["order_status"] = df["order_status"].str.strip().str.title()

            cleaned_count = len(df)
            dropped = original_count - cleaned_count
            log.info(f"{object_name}: {original_count} rows → {cleaned_count} after cleaning ({dropped} dropped)")

            # Load into PostgreSQL
            insert_sql = """
                INSERT INTO sales_transactions (
                    transaction_id, transaction_date, customer_id, customer_name,
                    customer_email, customer_region, product_id, product_name, product_category,
                    quantity, unit_price, total_amount, discount, payment_method, order_status
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """
            inserted = 0
            failed = 0
            with conn.cursor() as cur:
                for _, row in df.iterrows():
                    try:
                        cur.execute(
                            insert_sql,
                            (
                                row["transaction_id"],
                                row["transaction_date"],
                                row["customer_id"],
                                row["customer_name"],
                                row["customer_email"],
                                row["customer_region"],
                                row["product_id"],
                                row["product_name"],
                                row["product_category"],
                                row["quantity"],
                                row["unit_price"],
                                row["total_amount"],
                                row["discount"],
                                row["payment_method"],
                                row["order_status"],
                            ),
                        )
                        inserted += 1
                    except Exception as row_err:
                        log.error(f"Row error: {row_err}")
                        failed += 1

                # Log pipeline run
                cur.execute(
                    """
                    INSERT INTO pipeline_runs
                        (run_id, dag_id, file_processed, records_ingested, records_failed, status, started_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """,
                    (
                        context["run_id"],
                        context["dag"].dag_id,
                        object_name,
                        inserted,
                        failed,
                        "success",
                        context["data_interval_start"],
                        datetime.now(),
                    ),
                )

                conn.commit()
                log.info(f"Loaded {inserted} rows, {failed} failed from {object_name}")
                total_inserted += inserted
                total_failed += failed

                # Archive processed file
            archive_name = object_name.replace("incoming/", "processed/")
            # Fix: encode str→bytes so len() returns byte-count, not char-count
            csv_bytes = csv_data.encode("utf-8")
            source_bucket = _get_minio_config()["bucket"]
            client.put_object(
                ARCHIVE_BUCKET,
                archive_name,
                io.BytesIO(csv_bytes),
                len(csv_bytes),
                content_type="text/csv",
            )
            client.remove_object(source_bucket, object_name)
            log.info(f"Archived {object_name} → {ARCHIVE_BUCKET}/{archive_name}")
            files_processed += 1

        except Exception as e:
            log.error(f"Failed to process {object_name}: {e}")
            conn.rollback()
        finally:
            conn.close()  # always close, even on partial failure

    summary = {
        "files_processed": files_processed,
        "total_records": total_inserted,
        "total_failed": total_failed,
    }
    ti.xcom_push(key="processing_summary", value=summary)
    return summary


def refresh_materialized_views(**context):
    """Refresh PostgreSQL materialized views for Metabase dashboards."""
    conn = get_db_connection()
    try:
        with conn.cursor() as cur:
            cur.execute("REFRESH MATERIALIZED VIEW daily_sales_summary;")
            conn.commit()
            log.info("Refreshed daily_sales_summary materialized view")
    except Exception as e:
        log.error(f"Failed to refresh views: {e}")
        conn.rollback()
    finally:
        conn.close()


def log_pipeline_summary(**context):
    """Log final pipeline execution summary."""
    ti = context["ti"]
    summary = ti.xcom_pull(key="processing_summary", task_ids="process_and_load")
    files = ti.xcom_pull(key="files_to_process", task_ids="check_for_new_files")

    log.info("=" * 50)
    log.info("PIPELINE EXECUTION SUMMARY")
    log.info("=" * 50)
    log.info(f"Run ID:           {context['run_id']}")
    log.info(f"Execution Date:   {context['data_interval_start']}")
    log.info(f"Files Found:      {len(files) if files else 0}")
    log.info(f"Files Processed:  {summary.get('files_processed', 0) if summary else 0}")
    log.info(f"Records Loaded:   {summary.get('total_records', 0) if summary else 0}")
    log.info(f"Records Failed:   {summary.get('total_failed', 0) if summary else 0}")
    log.info("=" * 50)


# Define the DAG
with DAG(
    dag_id="sales_data_pipeline",
    description="ETL pipeline: MinIO → PostgreSQL → Metabase",
    default_args=DEFAULT_ARGS,
    start_date=days_ago(1),
    schedule_interval="@hourly",
    catchup=False,
    tags=["sales", "etl", "production"],
    doc_md=__doc__,
    is_paused_upon_creation=False,
) as dag:

    start = EmptyOperator(task_id="start")

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command="python /opt/airflow/scripts/generate_data.py --upload --rows 1000 --files 3",
    )

    check_files = PythonOperator(
        task_id="check_for_new_files",
        python_callable=check_for_new_files,
    )
    process_load = PythonOperator(
        task_id="process_and_load",
        python_callable=process_and_load_files,
    )
    refresh_views = PythonOperator(
        task_id="refresh_materialized_views",
        python_callable=refresh_materialized_views,
    )
    log_summary = PythonOperator(
        task_id="log_summary",
        python_callable=log_pipeline_summary,
    )

    validate_data = BashOperator(
        task_id="validate_data",
        bash_command="python /opt/airflow/scripts/validate_data_flow.py",
        env={
            "DATA_DB_HOST": os.getenv("DATA_DB_HOST", "postgres"),
            "MINIO_ENDPOINT": os.getenv("MINIO_ENDPOINT", "minio:9000"),
            "AIRFLOW_URL": "http://airflow-webserver:8080/health",
            "METABASE_URL": "http://metabase:3000/api/health",
        },
    )

    end = EmptyOperator(task_id="end")

    # Define task dependencies
    start >> generate_data >> check_files >> process_load >> refresh_views >> log_summary >> validate_data >> end
