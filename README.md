# Employee Data Pipeline with Apache Airflow

[![IMAGE ALT TEXT HERE](https://img.youtube.com/vi/KVDSKJVEh3U/0.jpg)](https://www.youtube.com/watch?v=KVDSKJVEh3U)

## Overview
Automated data pipeline using Apache Airflow to process employee data through ingestion, cleaning, and analysis stages in MySQL.

## Features
- CSV data ingestion to MySQL
- Data cleaning and standardization
- Seven analytical views (salary, performance, hiring trends)
- Robust error handling and logging


### Requirements
- MySQL Server
- Apache Airflow
- Python 3.x with mysql-connector-python and pandas

### Setup
```bash
./airflow_orchestration.sh  # Installs dependencies, configures MySQL, and starts Airflow
```

Access Airflow UI: http://localhost:8080 (admin/admin)

or 

use GitHub Codespace.

## Pipeline Structure
```
ingestion_task >> cleaning_task >> [seven_analysis_tasks]
```

## Troubleshooting
- Connection issues: Check MySQL service, user permissions, and file access rights
- Silent failures: Examine logs for permission errors despite successful task completion

## Files
- `setup.sh`: all in one project running script
- `employee_data_source.csv`: Sample data