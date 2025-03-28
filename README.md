# Airflow Data Pipeline Orchestration Project

This project orchestrates SQL scripts for data ingestion, cleaning, and transformation using Apache Airflow and MySQL.

## Project Structure

```
├── config/                 # Airflow configuration files
├── dags/                   # Airflow DAG files
│   └── sql_data_pipeline.py # Main DAG definition
├── logs/                   # Airflow logs
├── plugins/                # Airflow plugins
├── scripts/                # SQL and other scripts
│   ├── cleaning/           # Data cleaning SQL scripts
│   ├── ingestion/          # Data ingestion SQL scripts
│   ├── mysql-init/         # MySQL initialization scripts
│   └── transformation/     # Data transformation SQL scripts
├── .env                    # Environment variables
├── docker-compose.yml      # Docker Compose configuration
└── README.md               # Project documentation
```

## Setup Instructions

1. **Prerequisites**
   - Docker Desktop for Mac
   - Git

2. **Environment Setup**
   - Clone this repository
   - Update the `.env` file with your email and generate a Fernet key
   - Replace placeholder SQL scripts with your actual scripts from previous assignments

3. **Starting the Services**
   ```bash
   # Start Docker containers
   docker-compose up -d
   
   # Check container status
   docker-compose ps
   ```

4. **Access Airflow Web UI**
   - Open your browser and navigate to http://localhost:9090
   - Default username: airflow
   - Default password: airflow

5. **Set Up MySQL Connection in Airflow**
   - Go to Admin → Connections
   - Add a new connection:
     - Conn Id: mysql_default
     - Conn Type: MySQL
     - Host: mysql
     - Schema: airflow
     - Login: airflow
     - Password: airflow
     - Port: 3306

6. **Running the DAG**
   - Enable the DAG in the Airflow UI
   - Trigger it manually or wait for scheduled execution

## Monitoring

- View DAG progress in the Airflow UI
- Check task logs for detailed information
- Monitor MySQL database using a client like MySQL Workbench

## Troubleshooting

- Check container logs: `docker-compose logs -f airflow-webserver`
- Ensure MySQL is accessible: `docker-compose exec mysql mysql -u airflow -p`
- Verify SQL scripts exist in the correct locations

## Task Documentation

- **Ingestion Task**: Loads initial data into the employees table
- **Cleaning Task**: Standardizes data and removes inconsistencies
- **Department Analysis**: Aggregates employee data by department
- **Performance Analysis**: Categorizes employees by performance rating

## Author

- Your Name
