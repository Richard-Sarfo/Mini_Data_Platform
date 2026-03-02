#!/usr/bin/env python3
"""
Sales Data Generator
Generates realistic fake sales data and uploads to MinIO.
Usage: python generate_data.py [--rows 1000] [--files 3] [--upload]
"""

import argparse
import csv
import os
import random
import uuid
from datetime import datetime, timedelta
from pathlib import Path

# Optional MinIO upload
try:
    from minio import Minio
    MINIO_AVAILABLE = True
except ImportError:
    MINIO_AVAILABLE = False

PRODUCTS = [
    ("P001", "Laptop Pro 15", "Electronics", 1299.99),
    ("P002", "Wireless Mouse", "Electronics", 29.99),
    ("P003", "USB-C Hub", "Electronics", 49.99),
    ("P004", "Mechanical Keyboard", "Electronics", 149.99),
    ("P005", "Monitor 27\"", "Electronics", 449.99),
    ("P006", "Office Chair", "Furniture", 399.99),
    ("P007", "Standing Desk", "Furniture", 699.99),
    ("P008", "Notebook Set", "Stationery", 14.99),
    ("P009", "Pen Pack", "Stationery", 8.99),
    ("P010", "Whiteboard", "Stationery", 59.99),
    ("P011", "Coffee Maker", "Kitchen", 89.99),
    ("P012", "Water Bottle", "Kitchen", 24.99),
    ("P013", "Headphones", "Electronics", 199.99),
    ("P014", "Webcam HD", "Electronics", 79.99),
    ("P015", "Desk Lamp", "Furniture", 49.99),
]

CUSTOMERS = [
    ("C001", "Alice Johnson", "alice@techcorp.com", "North"),
    ("C002", "Bob Smith", "bob@innovate.io", "South"),
    ("C003", "Carol White", "carol@startup.co", "East"),
    ("C004", "David Lee", "david@enterprise.net", "West"),
    ("C005", "Emma Davis", "emma@consulting.org", "North"),
    ("C006", "Frank Miller", "frank@solutions.com", "South"),
    ("C007", "Grace Wilson", "grace@digital.io", "East"),
    ("C008", "Henry Brown", "henry@ventures.co", "West"),
    ("C009", "Isabella Jones", "isabella@agency.net", "North"),
    ("C010", "James Garcia", "james@services.org", "South"),
    ("C011", "Karen Martinez", "karen@systems.com", "East"),
    ("C012", "Liam Anderson", "liam@platform.io", "West"),
    ("C013", "Mia Thomas", "mia@cloud.co", "North"),
    ("C014", "Noah Jackson", "noah@data.net", "South"),
    ("C015", "Olivia Taylor", "olivia@analytics.org", "East"),
]

PAYMENT_METHODS = ["Credit Card", "Debit Card", "PayPal", "Bank Transfer", "Corporate Account"]
ORDER_STATUSES = ["Completed", "Completed", "Completed", "Pending", "Refunded"]


def random_date(start_days_ago=365, end_days_ago=0):
    """Generate a random datetime between `start` and `end`"""
    start = datetime.now() - timedelta(days=start_days_ago)
    end = datetime.now() - timedelta(days=end_days_ago)
    delta = end - start
    random_days = random.randint(0, delta.days)
    return (start + timedelta(days=random_days)).date()


def generate_transaction():
    """Generate a single sales transaction record."""
    product = random.choice(PRODUCTS)
    customer = random.choice(CUSTOMERS)
    quantity = random.randint(1, 10)
    unit_price = product[3]
    discount = round(random.choice([0, 0, 0, 5, 10, 15, 20]) * unit_price / 100, 2)
    total = round((unit_price * quantity) - discount, 2)

    return {
        "transaction_id": f"TXN-{uuid.uuid4().hex[:12].upper()}",
        "transaction_date": random_date(),
        "customer_id": customer[0],
        "customer_name": customer[1],
        "customer_email": customer[2],
        "customer_region": customer[3],
        "product_id": product[0],
        "product_name": product[1],
        "product_category": product[2],
        "quantity": quantity,
        "unit_price": unit_price,
        "total_amount": total,
        "discount": discount,
        "payment_method": random.choice(PAYMENT_METHODS),
        "order_status": random.choice(ORDER_STATUSES),
    }


def generate_csv(filepath: str, num_rows: int = 500):
    """Generate a CSV file with fake sales data."""
    Path(filepath).parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(generate_transaction().keys())

    with open(filepath, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        for _ in range(num_rows):
            writer.writerow(generate_transaction())

    print(f"Generated {num_rows} rows → {filepath}")
    return filepath


def upload_to_minio(filepath: str, bucket: str = "sales-data"):
    """Upload a CSV file to MinIO."""
    if not MINIO_AVAILABLE:
        print("minio package not installed. Skipping upload.")
        return

    endpoint = os.getenv("MINIO_ENDPOINT", "localhost:9000")
    access_key = os.getenv("MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.getenv("MINIO_SECRET_KEY", "minioadmin123")

    client = Minio(endpoint, access_key=access_key, secret_key=secret_key, secure=False)

    # Ensure bucket exists
    if not client.bucket_exists(bucket):
        client.make_bucket(bucket)

    filename = Path(filepath).name
    object_name = f"incoming/{filename}"
    client.fput_object(bucket, object_name, filepath)
    print(f"Uploaded {filename} → minio://{bucket}/{object_name}")


def main():
    parser = argparse.ArgumentParser(description="Generate fake sales data")
    parser.add_argument("--rows", type=int, default=500, help="Rows per file")
    parser.add_argument("--files", type=int, default=3, help="Number of files")
    parser.add_argument("--upload", action="store_true", help="Upload to MinIO")
    parser.add_argument("--output-dir", default="./data/sample", help="Output directory")
    args = parser.parse_args()

    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")

    for i in range(1, args.files + 1):
        filename = f"sales_batch_{timestamp}_{i:02d}.csv"
        filepath = os.path.join(args.output_dir, filename)
        generate_csv(filepath, args.rows)

        if args.upload:
            upload_to_minio(filepath)

    print(f"\nDone! Generated {args.files} file(s) with {args.rows} rows each.")
    if not args.upload:
        print("Tip: Add --upload to push files directly to MinIO")


if __name__ == "__main__":
    main()
