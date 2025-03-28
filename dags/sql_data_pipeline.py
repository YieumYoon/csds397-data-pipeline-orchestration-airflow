from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.mysql.operators.mysql import MySqlOperator
from airflow.utils.email import send_email

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
    schedule_interval='*/1 * * * *',
    start_date=datetime(2025, 3, 28),
    catchup=False,
    tags=['sql', 'data_pipeline'],
)

# Define tasks
ingestion_task = MySqlOperator(
    task_id='data_ingestion',
    mysql_conn_id='mysql_default',
    sql="""
    USE project_db;
    
    -- This is a placeholder - replace with your actual ingestion logic
    INSERT INTO employees (name, department, salary, performance_rating, hire_date)
    VALUES 
    ('John Doe', 'Engineering', 90000.00, 4.2, '2020-01-15'),
    ('Jane Smith', 'Marketing', 85000.00, 4.5, '2019-03-20'),
    ('Michael Johnson', 'Finance', 110000.00, 4.0, '2018-11-10'),
    ('Emily Williams', 'HR', 75000.00, 3.8, '2021-05-05'),
    ('David Brown', 'Engineering', 95000.00, 4.1, '2020-07-22'),
    ('Sarah Miller', 'Sales', 80000.00, 3.9, '2019-09-18'),
    ('Robert Wilson', 'Engineering', 92000.00, 3.7, '2021-02-28'),
    ('Jennifer Moore', 'Marketing', 82000.00, 4.3, '2018-06-12'),
    ('William Taylor', 'Finance', 105000.00, 4.2, '2020-11-30'),
    ('Amanda Anderson', 'HR', 78000.00, 3.6, '2021-01-07');
    """,
    database='project_db',
    dag=dag,
)

cleaning_task = MySqlOperator(
    task_id='data_cleaning',
    mysql_conn_id='mysql_default',
    sql="""
    USE project_db;
    
    -- This is a placeholder - replace with your actual cleaning logic
    -- Example: Fix any NULL values in the salary column
    UPDATE employees
    SET salary = 0
    WHERE salary IS NULL;
    
    -- Example: Remove any duplicate records
    CREATE TEMPORARY TABLE temp_employees AS
    SELECT DISTINCT * FROM employees;
    
    TRUNCATE TABLE employees;
    
    INSERT INTO employees
    SELECT * FROM temp_employees;
    
    -- Example: Standardize department names
    UPDATE employees
    SET department = 'Engineering'
    WHERE department IN ('engineering', 'Engineering Dept', 'Eng');
    
    UPDATE employees
    SET department = 'Marketing'
    WHERE department IN ('marketing', 'Marketing Dept', 'Mktg');
    
    UPDATE employees
    SET department = 'Finance'
    WHERE department IN ('finance', 'Finance Dept', 'Fin');
    
    UPDATE employees
    SET department = 'HR'
    WHERE department IN ('hr', 'Human Resources', 'Human Resource');
    """,
    database='project_db',
    dag=dag,
)

transform_task_1 = MySqlOperator(
    task_id='transform_department_analysis',
    mysql_conn_id='mysql_default',
    sql="""
    USE project_db;
    
    -- Create a department analysis table
    CREATE TABLE IF NOT EXISTS department_analysis (
        department VARCHAR(100) PRIMARY KEY,
        employee_count INT,
        avg_salary DECIMAL(10, 2),
        avg_performance DECIMAL(3, 2),
        total_salary_budget DECIMAL(12, 2)
    );
    
    -- Clear any existing data
    TRUNCATE TABLE department_analysis;
    
    -- Populate the department analysis table
    INSERT INTO department_analysis (department, employee_count, avg_salary, avg_performance, total_salary_budget)
    SELECT 
        department,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        AVG(performance_rating) as avg_performance,
        SUM(salary) as total_salary_budget
    FROM employees
    GROUP BY department
    ORDER BY total_salary_budget DESC;
    """,
    database='project_db',
    dag=dag,
)

transform_task_2 = MySqlOperator(
    task_id='transform_performance_analysis',
    mysql_conn_id='mysql_default',
    sql="""
    USE project_db;
    
    -- Create a performance analysis table
    CREATE TABLE IF NOT EXISTS performance_analysis (
        performance_bracket VARCHAR(20) PRIMARY KEY,
        employee_count INT,
        avg_salary DECIMAL(10, 2),
        min_salary DECIMAL(10, 2),
        max_salary DECIMAL(10, 2)
    );
    
    -- Clear any existing data
    TRUNCATE TABLE performance_analysis;
    
    -- Populate the performance analysis table
    INSERT INTO performance_analysis (performance_bracket, employee_count, avg_salary, min_salary, max_salary)
    SELECT 
        CASE 
            WHEN performance_rating >= 4.5 THEN 'Outstanding (4.5+)'
            WHEN performance_rating >= 4.0 THEN 'Excellent (4.0-4.4)'
            WHEN performance_rating >= 3.5 THEN 'Good (3.5-3.9)'
            WHEN performance_rating >= 3.0 THEN 'Average (3.0-3.4)'
            ELSE 'Below Average (<3.0)'
        END as performance_bracket,
        COUNT(*) as employee_count,
        AVG(salary) as avg_salary,
        MIN(salary) as min_salary,
        MAX(salary) as max_salary
    FROM employees
    GROUP BY performance_bracket
    ORDER BY 
        CASE performance_bracket
            WHEN 'Outstanding (4.5+)' THEN 1
            WHEN 'Excellent (4.0-4.4)' THEN 2
            WHEN 'Good (3.5-3.9)' THEN 3
            WHEN 'Average (3.0-3.4)' THEN 4
            ELSE 5
        END;
    """,
    database='project_db',
    dag=dag,
)

# Set task dependencies - all transformation tasks depend on the cleaning task,
# and the cleaning task depends on the ingestion task
ingestion_task >> cleaning_task >> [transform_task_1, transform_task_2]
