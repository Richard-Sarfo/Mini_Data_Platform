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