#!/usr/bin/env python3
"""
Sales Data Generator
Generates realistic fake sales data and uploads to MinIO.
Usage: python generate_data.py [--rows 1000] [--files 3] [--upload]
"""

import csv
import random
import uuid
import argparse
import os
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
