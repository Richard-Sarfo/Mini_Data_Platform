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

