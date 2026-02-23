-- Create the sales database and user
CREATE DATABASE salesdb;
CREATE DATABASE metabase;

CREATE USER datauser WITH PASSWORD 'datapassword';
GRANT ALL PRIVILEGES ON DATABASE salesdb TO datauser;
GRANT ALL PRIVILEGES ON DATABASE metabase TO airflow;

-- Connect to salesdb and create schema
\c salesdb

GRANT ALL ON SCHEMA public TO datauser;

-- Sales transactions table
CREATE TABLE IF NOT EXISTS sales_transactions (
    id SERIAL PRIMARY KEY,
    transaction_id VARCHAR(50) UNIQUE NOT NULL,
    transaction_date DATE NOT NULL,
    customer_id VARCHAR(50),
    customer_name VARCHAR(100),
    customer_email VARCHAR(150),
    customer_region VARCHAR(50),
    product_id VARCHAR(50),
    product_name VARCHAR(100),
    product_category VARCHAR(50),
    quantity INTEGER,
    unit_price DECIMAL(10, 2),
    total_amount DECIMAL(10, 2),
    discount DECIMAL(5, 2),
    payment_method VARCHAR(50),
    order_status VARCHAR(30),
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Daily sales summary view
CREATE MATERIALIZED VIEW IF NOT EXISTS daily_sales_summary AS
SELECT
    transaction_date,
    customer_region,
    product_category,
    COUNT(*) AS num_transactions,
    SUM(quantity) AS total_units,
    SUM(total_amount) AS total_revenue,
    AVG(total_amount) AS avg_order_value,
    SUM(discount) AS total_discounts
FROM sales_transactions
GROUP BY transaction_date, customer_region, product_category;

-- Product performance view
CREATE VIEW IF NOT EXISTS product_performance AS
SELECT
    product_id,
    product_name,
    product_category,
    COUNT(*) AS total_orders,
    SUM(quantity) AS total_units_sold,
    SUM(total_amount) AS total_revenue,
    AVG(unit_price) AS avg_price,
    AVG(discount) AS avg_discount
FROM sales_transactions
GROUP BY product_id, product_name, product_category;

-- Customer metrics view
CREATE VIEW IF NOT EXISTS customer_metrics AS
SELECT
    customer_id,
    customer_name,
    customer_region,
    COUNT(*) AS num_orders,
    SUM(total_amount) AS lifetime_value,
    AVG(total_amount) AS avg_order_value,
    MIN(transaction_date) AS first_purchase,
    MAX(transaction_date) AS last_purchase
FROM sales_transactions
GROUP BY customer_id, customer_name, customer_region;

-- Pipeline run log
CREATE TABLE IF NOT EXISTS pipeline_runs (
    id SERIAL PRIMARY KEY,
    run_id VARCHAR(100) NOT NULL,
    dag_id VARCHAR(100),
    file_processed VARCHAR(255),
    records_ingested INTEGER,
    records_failed INTEGER,
    status VARCHAR(20),
    started_at TIMESTAMP,
    completed_at TIMESTAMP,
    notes TEXT
);

GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO datauser;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO datauser;