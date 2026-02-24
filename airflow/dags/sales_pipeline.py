"""
Sales Data Pipeline DAG
=======================
Flow: MinIO (CSV files) → Clean & Validate → PostgreSQL → Refresh Views

Schedule: Every hour
"""


import io
import os
import logging
from datetime import datetime, timedelta

import pandas as pd
import psycopg2
from minio import Minio
from minio.error import S3Error

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.dates import days_ago

log = logging.getLogger(__name__)


MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "minio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY", "minioadmin123")
SOURCE_BUCKET = os.getenv("MINIO_BUCKET", "sales-data")
ARCHIVE_BUCKET = "archive"

DB_CONFIG = {
    "host": os.getenv("DATA_DB_HOST", "postgres"),
    "port": int(os.getenv("DATA_DB_PORT", 5432)),
    "dbname": os.getenv("DATA_DB_NAME", "salesdb"),
    "user": os.getenv("DATA_DB_USER", "datauser"),
    "password": os.getenv("DATA_DB_PASSWORD", "datapassword"),
}

DEFAULT_ARGS = {
    "owner": "data-team",
    "depends_on_past": False,
    "email": [" richard.sarfo@amalitech.com"],
    "email_on_failure": True,
    "email_on_retry": False,
    "retries": 2,
    "retry_delay": timedelta(minutes=5),
}

EXPECTED_COLUMNS = [
    "transaction_id", "transaction_date", "customer_id", "customer_name",
    "customer_email", "customer_region", "product_id", "product_name",
    "product_category", "quantity", "unit_price", "total_amount",
    "discount", "payment_method", "order_status",
]

def get_minio_client() -> Minio:
    return Minio(MINIO_ENDPOINT, access_key=MINIO_ACCESS_KEY,
                 secret_key=MINIO_SECRET_KEY, secure=False)


def get_db_connection():
    return psycopg2.connect(**DB_CONFIG)


def check_for_new_files(**context):
    """Scan MinIO 'incoming/' prefix for CSV files to process."""
    client = get_minio_client()
    
    try:
        objects = list(client.list_objects(SOURCE_BUCKET, prefix="incoming/", recursive=True))
    except S3Error as e:
        log.warning(f"MinIO error: {e}")
        objects = []

    csv_files = [obj.object_name for obj in objects if obj.object_name.endswith(".csv")]

    if not csv_files:
        log.info("No new CSV files found in MinIO.")
    else:
        log.info(f"Found {len(csv_files)} file(s): {csv_files}")

    context["ti"].xcom_push(key="files_to_process", value=csv_files)
    return csv_files


def process_and_load_files(**context):
    """Download, clean, validate, and load each CSV file into PostgreSQL."""
    ti = context["ti"]
    files = ti.xcom_pull(key="files_to_process", task_ids="check_for_new_files")

    if not files:
        log.info("No files to process.")
        return {"processed": 0, "total_records": 0}

    client = get_minio_client()
    conn = get_db_connection()

    total_inserted = 0
    total_failed = 0
    files_processed = 0


    for object_name in files:
        log.info(f"Processing: {object_name}")

        try:
            # Download from MinIO
            response = client.get_object(SOURCE_BUCKET, object_name)
            csv_data = response.read().decode("utf-8")
            response.close()
            
            #load into dataframe
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
                        cur.execute(insert_sql, (
                            row["transaction_id"], row["transaction_date"], row["customer_id"],
                            row["customer_name"], row["customer_email"], row["customer_region"],
                            row["product_id"], row["product_name"], row["product_category"],
                            row["quantity"], row["unit_price"], row["total_amount"],
                            row["discount"], row["payment_method"], row["order_status"]
                        ))
                        inserted += 1
                    except Exception as row_err:
                        log.error(f"Row error: {row_err}")
                        failed += 1

                # Log pipeline run
                cur.execute("""
                    INSERT INTO pipeline_runs
                        (run_id, dag_id, file_processed, records_ingested, records_failed, status, started_at, completed_at)
                    VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
                """, (
                    context["run_id"], context["dag"].dag_id, object_name,
                    inserted, failed, "success",
                    context["data_interval_start"], datetime.now(),
                ))

                conn.commit()
                log.info(f"Loaded {inserted} rows, {failed} failed from {object_name}")
                total_inserted += inserted
                total_failed += failed

                # Archive processed file 
            archive_name = object_name.replace("incoming/", "processed/")
            csv_bytes = csv_data
            client.put_object(ARCHIVE_BUCKET, archive_name,
                              io.BytesIO(csv_bytes), len(csv_bytes),
                              content_type="text/csv")
            client.remove_object(SOURCE_BUCKET, object_name)
            log.info(f"Archived {object_name} → {ARCHIVE_BUCKET}/{archive_name}")
            files_processed += 1

        except Exception as e:
            log.error(f"Failed to process {object_name}: {e}")
            conn.rollback()

    conn.close()

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