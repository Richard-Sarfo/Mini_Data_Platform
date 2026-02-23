#!/usr/bin/env python3
"""
Data Flow Validation Script
============================
Validates that data moves correctly through:
MinIO (ingest) → Airflow (process) → PostgreSQL (store) → Metabase (visualize)

Exit codes:
  0 = all checks passed
  1 = one or more checks failed
"""

import sys
import time
import json
import urllib.request
import urllib.error
import logging
import os
import psycopg2
from minio import Minio

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

def check_postgres():

    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "localhost"),
        port=int(os.getenv("DATA_DB_PORT", 5432)),
        dbname=os.getenv("DATA_DB_NAME", "salesdb"),
        user=os.getenv("DATA_DB_USER", "datauser"),
        password=os.getenv("DATA_DB_PASSWORD", "datapassword"),
        connect_timeout=5,
    )
    conn.close()
    return True

def check_postgres_schema():
    
    conn = psycopg2.connect(
        host=os.getenv("DATA_DB_HOST", "localhost"),
        port=int(os.getenv("DATA_DB_PORT", 5432)),
        dbname=os.getenv("DATA_DB_NAME", "salesdb"),
        user=os.getenv("DATA_DB_USER", "datauser"),
        password=os.getenv("DATA_DB_PASSWORD", "datapassword"),
    )
    with conn.cursor() as cur:
        cur.execute("""
            SELECT table_name FROM information_schema.tables
            WHERE table_schema = 'public'
            AND table_name IN ('sales_transactions', 'pipeline_runs')
        """)
        tables = {r[0] for r in cur.fetchall()}
    conn.close()
    assert "sales_transactions" in tables, "Missing sales_transactions table"
    assert "pipeline_runs" in tables, "Missing pipeline_runs table"
    return True