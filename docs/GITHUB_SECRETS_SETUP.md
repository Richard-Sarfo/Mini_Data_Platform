# GitHub Secrets Setup Guide

This document explains how to configure GitHub repository secrets for the CI/CD pipeline.

## Required Secrets

The following secrets must be configured in your GitHub repository for the CI/CD pipeline to work:

### PostgreSQL Credentials
- `POSTGRES_USER` - PostgreSQL admin user (default: `airflow`)
- `POSTGRES_PASSWORD` - PostgreSQL admin password (default: `airflow`)
- `POSTGRES_DB` - PostgreSQL admin database (default: `airflow`)

### Data Database Credentials
- `DATA_DB_NAME` - Sales data database name (default: `salesdb`)
- `DATA_DB_USER` - Sales data database user (default: `datauser`)
- `DATA_DB_PASSWORD` - Sales data database password (default: `datapassword`)

### MinIO Credentials
- `MINIO_ACCESS_KEY` - MinIO access key (default: `minioadmin`)
- `MINIO_SECRET_KEY` - MinIO secret key (default: `minioadmin123`)

### Airflow Web UI Credentials
- `AIRFLOW_WWW_USER` - Airflow web UI username (default: `admin`)
- `AIRFLOW_WWW_PASSWORD` - Airflow web UI password (default: `admin`)

## How to Add Secrets

1. Navigate to your GitHub repository
2. Click on **Settings** tab
3. In the left sidebar, click **Secrets and variables** → **Actions**
4. Click **New repository secret** button
5. Add each secret with its name and value
6. Click **Add secret**

## Example Values (for development/testing)

```bash
POSTGRES_USER=airflow
POSTGRES_PASSWORD=airflow
POSTGRES_DB=airflow
DATA_DB_NAME=salesdb
DATA_DB_USER=datauser
DATA_DB_PASSWORD=datapassword
MINIO_ACCESS_KEY=minioadmin
MINIO_SECRET_KEY=minioadmin123
AIRFLOW_WWW_USER=admin
AIRFLOW_WWW_PASSWORD=admin
```

## Production Recommendations

For production environments, use strong, unique passwords:
- Use a password manager to generate secure passwords
- Minimum 16 characters with mixed case, numbers, and symbols
- Never commit credentials to the repository
- Rotate credentials regularly
- Use different credentials for each environment (dev, staging, prod)

## Verifying Secrets

After adding secrets, you can verify they're configured correctly by:
1. Going to **Settings** → **Secrets and variables** → **Actions**
2. You should see all secret names listed (values are hidden)
3. Trigger a workflow run to test the configuration

## Troubleshooting

If the CI/CD pipeline fails with authentication errors:
- Verify all required secrets are added
- Check for typos in secret names (they are case-sensitive)
- Ensure secret values don't have extra spaces or newlines
- Check the workflow logs for specific error messages
