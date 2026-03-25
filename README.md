# Mini Data Platform 🚀

A robust, containerized end-to-end data platform for sales data processing, validation, and visualization.

### Data Flow Steps:
1.  **Data Generation**: `generate_data.py` creates realistic fake sales records and uploads them to the `incoming/` prefix in MinIO.
2.  **Detection**: Airflow task `check_for_new_files` scans MinIO for any new `.csv` files.
3.  **ETL Process**: 
    - Downloads files from MinIO.
    - Performs automated cleaning (handling nulls, duplicates, data types).
    - Validates columns against a predefined schema.
    - Loads clean records into the `sales_transactions` table in PostgreSQL.
    - Moves processed files to the `processed/` prefix for archiving.
4.  **Reporting**: Airflow refreshes materialized views for fast analytical querying.
5.  **Visualization**: Metabase connects to PostgreSQL to serve interactive dashboards.

---

## 🛠 Prerequisites

- **Docker & Docker Compose**
- **Git**
- **Python 3.9+** (for local scripts/linting)

---

## 🚀 Setup Instructions

### 1. Clone the repository
```bash
git clone https://github.com/Richard-Sarfo/Mini_Data_Platform.git
cd Mini_Data_Platform
```

### 2. Start the services
```bash
docker-compose up -d
```
This command builds the custom Airflow image and starts:
- **PostgreSQL** (Port 5432)
- **MinIO** (Port 9000 API, 9001 Console)
- **Airflow Webserver** (Port 8080)
- **Airflow Scheduler**
- **Metabase** (Port 3000)

### 3. Access the Tools
| Tool | URL | Credentials |
| :--- | :--- | :--- |
| **Airflow** | [localhost:8080](http://localhost:8080) | From `.env` (default: `admin` / `admin`) |
| **MinIO Console** | [localhost:9001](http://localhost:9001) | From `.env` (default: `minioadmin` / `minioadmin123`) |
| **Metabase** | [localhost:3000](http://localhost:3000) | Setup wizard on first run |

### 4. Trigger the Pipeline
You can wait for the hourly schedule or trigger it manually:
- Open Airflow at [localhost:8080](http://localhost:8080).
- Unpause the `sales_data_pipeline` DAG.
- Click "Trigger DAG".

Alternatively, run the generator script locally to push data:
```bash
pip install minio
python scripts/generate_data.py --rows 100 --files 2 --upload
```

### 5. Run Health Checks
Use the built-in validation script to ensure all components are communicating correctly:
```bash
pip install psycopg2-binary minio
python scripts/validate_data_flow.py
```

---

## 🧪 CI/CD Pipeline

The project includes a GitHub Actions workflow (`.github/workflows/ci-cd.yml`) that performs:
- **Linting**: Black, isort, and Flake8 checks.
- **Docker Build**: Automated building of custom images.
- **Integration Testing**: Spin up the full stack in a headless environment, runs the ETL, and verifies records in the database.
- **Deployment**: Automated deployment notifications.

---

## 📁 Repository Structure

- `airflow/`: DAG definitions and Airflow-specific configuration.
- `scripts/`: Python utilities for data generation and platform validation.
- `postgres/`: Database initialization scripts and schemas.
- `data/`: Local sample data storage.
- `pyproject.toml`: Configuration for linting tools.
