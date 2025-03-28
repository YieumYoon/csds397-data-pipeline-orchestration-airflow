from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.operators.python import PythonOperator
from airflow.utils.email import send_email
import pandas as pd
import mysql.connector
from mysql.connector import Error
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def task_failure_callback(context):
    """Send an email when a task fails."""
    send_email(
        to=[context['dag'].default_args['email']],
        subject=f"Airflow Alert: {context['task_instance'].task_id} Failed",
        html_content=f"""
        Task {context['task_instance'].task_id} has failed.<br>
        DAG: {context['task_instance'].dag_id}<br>
        Execution Time: {context['execution_date']}<br>
        Log URL: {context['task_instance'].log_url}
        """,
    )

# Function to load CSV data into MySQL
def load_csv_to_mysql():
    try:
        # Read the CSV file
        csv_path = '/opt/airflow/employee_data_source.csv'
        df = pd.read_csv(csv_path)
        
        # Connect to MySQL
        conn = mysql.connector.connect(
            host='mysql',
            user='airflow',
            password='airflow',
            database='project_db'
        )
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Clear existing data
            cursor.execute("TRUNCATE TABLE employees_raw")
            
            # Insert data from CSV
            for _, row in df.iterrows():
                query = """
                INSERT INTO employees_raw 
                (employee_id, name, age, department, date_of_joining, years_of_experience, country, salary, performance_rating)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                """
                values = (
                    str(row['Employee Id']),
                    str(row['Name']),
                    str(row['Age']),
                    str(row['Department']),
                    str(row['Date of Joining']),
                    str(row['Years of Experience']),
                    str(row['Country']),
                    str(row['Salary']),
                    str(row['Performance Rating'])
                )
                cursor.execute(query, values)
                
            # Commit changes
            conn.commit()
            logger.info(f"Data loaded successfully into employees_raw table.")
            
    except Exception as e:
        logger.error(f"Error loading data: {e}")
        raise
    finally:
        if 'conn' in locals() and conn.is_connected():
            cursor.close()
            conn.close()

# Default arguments
default_args = {
    'owner': 'junsu',
    'depends_on_past': False,
    'email': ['your-email@example.com'],
    'email_on_failure': True,
    'email_on_retry': True,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
    'on_failure_callback': task_failure_callback,
}

# Define DAG
dag = DAG(
    'sql_data_pipeline',
    default_args=default_args,
    description='A DAG to orchestrate SQL scripts for data ingestion, cleaning, and transformation',
    schedule_interval='@daily',  # Changed from every minute to daily
    start_date=datetime(2025, 3, 28),
    catchup=False,
    tags=['sql', 'data_pipeline'],
)

# Task 1: Data Ingestion using Python to load CSV
ingest_data = PythonOperator(
    task_id='ingest_data',
    python_callable=load_csv_to_mysql,
    dag=dag,
)

# Task 2: Data Cleaning
cleaning_task = MySqlOperator(
    task_id='data_cleaning',
    mysql_conn_id='mysql_default',
    sql="""
    -- Clear the cleaned employees table
    TRUNCATE TABLE employees;
    
    -- Insert cleaned data
    INSERT INTO employees (
        employee_id, 
        name, 
        age, 
        department, 
        date_of_joining, 
        years_of_experience, 
        country, 
        salary, 
        performance_rating
    )
    SELECT 
        -- Clean employee_id - convert to integer
        CAST(NULLIF(employee_id, '') AS UNSIGNED) AS employee_id,
        
        -- Clean name - handle empty values
        CASE WHEN name = '' THEN 'Unknown' ELSE name END AS name,
        
        -- Clean age - convert to integer, handle non-numeric values
        CASE 
            WHEN age REGEXP '^[0-9]+$' THEN CAST(age AS UNSIGNED)
            ELSE NULL
        END AS age,
        
        -- Clean department - standardize department names
        CASE 
            WHEN LOWER(department) IN ('hr', 'h r', 'human resources', 'human resource') THEN 'HR'
            WHEN LOWER(department) IN ('it', 'information technology') THEN 'IT'
            WHEN LOWER(department) IN ('r&d', 'research', 'rnd', 'research and development') THEN 'R&D'
            WHEN LOWER(department) IN ('operations', 'oprations') THEN 'Operations'
            WHEN LOWER(department) IN ('cust support', 'customersupport', 'support', 'customer support') THEN 'Customer Support'
            WHEN LOWER(department) IN ('finanace', 'fin', 'finance') THEN 'Finance'
            WHEN LOWER(department) IN ('lgistics', 'logistics') THEN 'Logistics'
            WHEN LOWER(department) IN ('marketng', 'marketing') THEN 'Marketing'
            WHEN LOWER(department) IN ('sales', 'slaes') THEN 'Sales'
            WHEN LOWER(department) IN ('legal', 'legl') THEN 'Legal'
            ELSE department
        END AS department,
        
        -- Clean date_of_joining - convert various date formats to standard format
        CASE
            WHEN date_of_joining REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(date_of_joining, '%Y-%m-%d')
            WHEN date_of_joining REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(date_of_joining, '%Y/%m/%d')
            ELSE NULL
        END AS date_of_joining,
        
        -- Clean years_of_experience - convert to integer, handle non-numeric values
        CASE 
            WHEN years_of_experience REGEXP '^[0-9]+$' THEN CAST(years_of_experience AS UNSIGNED)
            ELSE NULL
        END AS years_of_experience,
        
        -- Clean country - standardize country names
        CASE 
            WHEN LOWER(country) LIKE '%glarastan%' THEN 'Glarastan'
            WHEN LOWER(country) LIKE '%hesperia%' THEN 'Hesperia'
            WHEN LOWER(country) LIKE '%vorastria%' THEN 'Vorastria'
            WHEN LOWER(country) LIKE '%velronia%' THEN 'Velronia'
            WHEN LOWER(country) LIKE '%mordalia%' THEN 'Mordalia'
            WHEN LOWER(country) LIKE '%drivania%' THEN 'Drivania'
            WHEN LOWER(country) LIKE '%tavlora%' THEN 'Tavlora'
            WHEN LOWER(country) LIKE '%zorathia%' THEN 'Zorathia'
            WHEN LOWER(country) LIKE '%xanthoria%' THEN 'Xanthoria'
            WHEN LOWER(country) LIKE '%luronia%' THEN 'Luronia'
            ELSE country
        END AS country,
        
        -- Clean salary - convert to decimal, handle non-numeric values
        CASE 
            WHEN salary REGEXP '^[0-9]+(\.[0-9]+)?$' THEN CAST(salary AS DECIMAL(10,2))
            ELSE NULL
        END AS salary,
        
        -- Clean performance_rating - standardize ratings
        CASE 
            WHEN LOWER(performance_rating) LIKE '%top%' THEN 'Top Performers'
            WHEN LOWER(performance_rating) LIKE '%high%' THEN 'High Performers'
            WHEN LOWER(performance_rating) LIKE '%average%' THEN 'Average Performers'
            WHEN LOWER(performance_rating) LIKE '%low%' THEN 'Low Performers'
            WHEN LOWER(performance_rating) LIKE '%poor%' THEN 'Poor Performers'
            ELSE performance_rating
        END AS performance_rating
    FROM employees_raw
    WHERE employee_id IS NOT NULL AND employee_id != '';
    """,
    database='project_db',
    dag=dag,
)

# Task 3: Department Analysis
transform_department = MySqlOperator(
    task_id='transform_department_analysis',
    mysql_conn_id='mysql_default',
    sql="""
    -- Clear existing data
    TRUNCATE TABLE department_analysis;
    
    -- Insert aggregated department data
    INSERT INTO department_analysis
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        AVG(age) as avg_age,
        AVG(years_of_experience) as avg_experience
    FROM employees
    WHERE department IS NOT NULL AND department != ''
    GROUP BY department;
    """,
    database='project_db',
    dag=dag,
)

# Task 4: Performance Analysis
transform_performance = MySqlOperator(
    task_id='transform_performance_analysis',
    mysql_conn_id='mysql_default',
    sql="""
    -- Clear existing data
    TRUNCATE TABLE performance_analysis;
    
    -- Insert aggregated performance data
    INSERT INTO performance_analysis
    SELECT 
        performance_rating as performance_category,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary
    FROM employees
    WHERE performance_rating IS NOT NULL AND performance_rating != ''
    GROUP BY performance_rating;
    """,
    database='project_db',
    dag=dag,
)

# Task 5: Country Salary Analysis
transform_country = MySqlOperator(
    task_id='transform_country_analysis',
    mysql_conn_id='mysql_default',
    sql="""
    -- Clear existing data
    TRUNCATE TABLE country_salary_report;
    
    -- Insert aggregated country salary data
    INSERT INTO country_salary_report
    SELECT 
        country,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MAX(salary) as max_salary,
        MIN(salary) as min_salary,
        MAX(salary) - MIN(salary) as salary_range
    FROM employees
    WHERE country IS NOT NULL AND country != ''
    GROUP BY country;
    """,
    database='project_db',
    dag=dag,
)

# Set task dependencies
ingest_data >> cleaning_task >> [transform_department, transform_performance, transform_country]
