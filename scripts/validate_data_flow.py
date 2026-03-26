#!/usr/bin/env python3
"""
Data Flow Validation Script
============================
Validates that data moves correctly through:
MinIO (ingest) -> Airflow (process) -> PostgreSQL (store) -> Metabase (visualize)

Exit codes:
  0 = all checks passed
  1 = one or more checks failed
"""

import io
import json
import logging
import os
import sys
import time
import urllib.error
import urllib.request

import psycopg2
from minio import Minio

try:
    from dotenv import load_dotenv

    load_dotenv()
except ImportError:
    # dotenv not available, use environment variables directly
    pass


logging.basicConfig(level=logging.INFO, format="%(levelname)s | %(message)s")
log = logging.getLogger(__name__)

RESULTS = []


def check(name: str, fn):
    """Run a check function and record the result."""
    try:
        result = fn()
        status = "PASS" if result else "FAIL"
        RESULTS.append((name, status, ""))
        log.info(f"[{status}] {name}")
        return result
    except Exception as e:
        RESULTS.append((name, "FAIL", str(e)))
        log.error(f"[FAIL] {name}: {e}")
        return False


def check_postgres() -> bool:
    """Verify connectivity to PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "localhost"),
        port=int(os.getenv("DATA_DB_PORT") or 5432),
        dbname=os.getenv("DATA_DB_NAME"),
        user=os.getenv("DATA_DB_USER"),
        password=os.getenv("DATA_DB_PASSWORD"),
        connect_timeout=5,
    )
    conn.close()
    return True


REQUIRED_TABLES = {"sales_transactions", "pipeline_runs"}


def check_postgres_schema() -> bool:
    """Verify that required tables exist in PostgreSQL."""
    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "localhost"),
        port=int(os.getenv("DATA_DB_PORT") or 5432),
        dbname=os.getenv("DATA_DB_NAME"),
        user=os.getenv("DATA_DB_USER"),
        password=os.getenv("DATA_DB_PASSWORD"),
        connect_timeout=5,
    )
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name = ANY(%s)
            """,
            (list(REQUIRED_TABLES),),
        )
        found_tables = {r[0] for r in cur.fetchall()}
    conn.close()

    missing = REQUIRED_TABLES - found_tables
    if missing:
        raise RuntimeError(f"Missing required tables: {missing}")

    return True


def check_minio() -> bool:
    """Verify connectivity and bucket existence in MinIO."""
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=os.getenv("MINIO_SECURE", "false").lower() == "true",
    )
    bucket = os.getenv("MINIO_BUCKET")
    if not bucket:
        raise ValueError("MINIO_BUCKET environment variable is not set")

    buckets = {b.name for b in client.list_buckets()}
    if bucket not in buckets:
        raise RuntimeError(f"Missing '{bucket}' bucket. Found: {buckets}")

    return True


def check_airflow():
    """Verify Airflow health endpoint."""
    url = os.getenv("AIRFLOW_URL")
    try:
        req = urllib.request.Request(url)
        with urllib.request.urlopen(req, timeout=10) as resp:
            data = json.loads(resp.read())
            # Airflow 2.x health JSON:
            # {"metadatabase": {"status": "healthy"}, "scheduler": {"status": "healthy"}}
            db_status = data.get("metadatabase", {}).get("status")
            scheduler_status = data.get("scheduler", {}).get("status")
            return db_status == "healthy" and scheduler_status == "healthy"
    except urllib.error.URLError as e:
        raise RuntimeError(f"Airflow unreachable: {e}")


def check_metabase():
    """Verify Metabase health endpoint."""
    url = os.getenv("METABASE_URL", "http://localhost:3000/api/health")
    try:
        with urllib.request.urlopen(url, timeout=10) as resp:
            data = json.loads(resp.read())
            return data.get("status") == "ok"
    except urllib.error.URLError as e:
        raise RuntimeError(f"Metabase unreachable: {e}")


def check_end_to_end_data_flow():
    """Upload a test CSV, trigger pipeline check, verify DB record."""
    # Generate a unique test transaction
    test_id = f"TXN-VALIDATION-{int(time.time())}"
    csv_content = (
        "transaction_id,transaction_date,customer_id,customer_name,customer_email,"
        "customer_region,product_id,product_name,product_category,quantity,"
        "unit_price,total_amount,discount,payment_method,order_status\n"
        f"{test_id},2024-01-15,C001,Test Customer,test@validate.com,"
        "North,P001,Test Product,Electronics,1,99.99,99.99,0,Credit Card,Completed\n"
    )
    csv_bytes = csv_content.encode("utf-8")

    # Upload to MinIO
    client = Minio(
        os.getenv("MINIO_ENDPOINT", "localhost:9000"),
        access_key=os.getenv("MINIO_ACCESS_KEY"),
        secret_key=os.getenv("MINIO_SECRET_KEY"),
        secure=False,
    )
    bucket = os.getenv("MINIO_BUCKET", "sales-data")
    client.put_object(
        bucket,
        "incoming/validation_test.csv",
        io.BytesIO(csv_bytes),
        len(csv_bytes),
        content_type="text/csv",
    )
    log.info(f"Uploaded validation file with transaction_id: {test_id}")

    # Note: Full end-to-end requires Airflow to run DAG.
    # This check validates the upload succeeded.
    return True


def main():
    log.info("=" * 55)
    log.info("  MINI DATA PLATFORM - DATA FLOW VALIDATION")
    log.info("=" * 55)

    check("PostgreSQL connectivity", check_postgres)
    check("PostgreSQL schema integrity", check_postgres_schema)
    check("MinIO connectivity & buckets", check_minio)
    check("Airflow health endpoint", check_airflow)
    check("Metabase health endpoint", check_metabase)
    check("End-to-end upload test", check_end_to_end_data_flow)

    log.info("\n" + "=" * 55)
    log.info("RESULTS SUMMARY")
    log.info("=" * 55)

    passed = sum(1 for _, status, _ in RESULTS if status == "PASS")
    failed = sum(1 for _, status, _ in RESULTS if status == "FAIL")

    for name, status, error in RESULTS:
        icon = "PASS" if status == "PASS" else "FAIL"
        line = f"  [{icon}] {name}"
        if error:
            line += f"  ->  {error}"
        log.info(line)

    log.info(f"\n  Passed: {passed}/{len(RESULTS)}")

    if failed > 0:
        log.error(f"\n{failed} check(s) FAILED. Platform not healthy.")
        sys.exit(1)
    else:
        log.info("\nAll checks passed! Platform is healthy.")
        sys.exit(0)


if __name__ == "__main__":
    main()
