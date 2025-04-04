#!/bin/bash

# Install MySQL if not installed (for Ubuntu/Debian-based systems)
sudo apt-get update
sudo apt-get install -y mysql-server
sudo service mysql restart

# Creating a service account (user) that will be used to connect Airflow to MySQL
sudo mysql -u root -e "DROP USER IF EXISTS 'user'@'%';"
sudo mysql -u root -e "CREATE USER IF NOT EXISTS 'user'@'%' IDENTIFIED WITH mysql_native_password BY 'password';"
sudo mysql -u root -e "CREATE DATABASE IF NOT EXISTS employee_db;"
sudo mysql -u root -e "GRANT ALL PRIVILEGES ON employee_db.* TO 'user'@'%';"
sudo mysql -u root -e "GRANT FILE ON *.* TO 'user'@'%';"
sudo mysql -u root -e "FLUSH PRIVILEGES;"

# Create the database tables
sudo mysql -u root -e "USE employee_db;

-- Create the raw employees table for initial data ingestion
CREATE TABLE IF NOT EXISTS employees_raw (
    employee_id VARCHAR(20),
    name VARCHAR(100),
    age VARCHAR(10),
    department VARCHAR(50),
    date_of_joining VARCHAR(20),
    years_of_experience VARCHAR(10),
    country VARCHAR(50),
    salary VARCHAR(20),
    performance_rating VARCHAR(50)
);

-- Create the cleaned employees table
CREATE TABLE IF NOT EXISTS employees (
    employee_id INT,
    name VARCHAR(100),
    age INT,
    department VARCHAR(50),
    date_of_joining DATE,
    years_of_experience INT,
    country VARCHAR(50),
    salary DECIMAL(10,2),
    performance_rating VARCHAR(50),
    PRIMARY KEY (employee_id)
);"

# Copy the employee data to a secure location that MySQL can read
# Create the directory if it doesn't exist
sudo mkdir -p /var/lib/mysql-files/

# Set proper permissions
sudo chmod 750 /var/lib/mysql-files/
sudo chown mysql:mysql /var/lib/mysql-files/

# Copy the CSV file to both MySQL directory and Airflow home directory
sudo cp -v employee_data_source.csv /var/lib/mysql-files/
sudo chmod 644 /var/lib/mysql-files/employee_data_source.csv
sudo chown mysql:mysql /var/lib/mysql-files/employee_data_source.csv

# Also copy to Airflow home directory where the Airflow user has access
cp -v employee_data_source.csv $HOME/airflow/employee_data_source.csv
chmod 644 $HOME/airflow/employee_data_source.csv

# Verify the file was copied
sudo ls -la /var/lib/mysql-files/

# Setting up Airflow and initializing it
pip install apache-airflow apache-airflow-providers-mysql pandas
airflow db init

# Create webserver_config.py and disable CSRF and authentication for demo purposes
cat > $HOME/airflow/webserver_config.py << EOF
WTF_CSRF_ENABLED = False
WTF_CSRF_TIME_LIMIT = None
AUTH_ROLE_PUBLIC = 'Admin'
EOF

# Disable loading default demo DAGs
sed -i 's/load_examples = True/load_examples = False/g' $HOME/airflow/airflow.cfg

# Create the DAG directory
AIRFLOW_DAG_PATH="$HOME/airflow/dags"
mkdir -p $AIRFLOW_DAG_PATH

# Create the Airflow DAG file for employee data pipeline with all Python-based tasks
cat > $AIRFLOW_DAG_PATH/employee_data_pipeline.py << EOF
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta
import pandas as pd
import mysql.connector
from mysql.connector import Error

# Function to execute SQL queries directly
def execute_sql(sql_query):
    try:
        # Connect to MySQL directly
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='user',
            password='password',
            database='employee_db'
        )
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Execute the SQL query
            for statement in sql_query.split(';'):
                if statement.strip():
                    cursor.execute(statement)
            
            # Commit changes
            conn.commit()
            
            # Close the connection
            cursor.close()
            conn.close()
            print("SQL query executed successfully")
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    except Exception as e:
        print(f"Other error occurred: {e}")

# Function to ingest data using Python
def ingest_data_to_mysql():
    try:
        # Read the CSV file
        # df = pd.read_csv('/var/lib/mysql-files/employee_data_source.csv')
        df = pd.read_csv('$HOME/airflow/employee_data_source.csv')
        
        # Connect to MySQL directly
        conn = mysql.connector.connect(
            host='127.0.0.1',
            user='user',
            password='password',
            database='employee_db'
        )
        
        if conn.is_connected():
            cursor = conn.cursor()
            
            # Clear the table
            cursor.execute("TRUNCATE TABLE employees_raw")
            
            # Insert data row by row
            for i, row in df.iterrows():
                sql = """INSERT INTO employees_raw (employee_id, name, age, department, 
                      date_of_joining, years_of_experience, country, salary, performance_rating) 
                      VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)"""
                cursor.execute(sql, tuple(row))
                
            # Commit changes
            conn.commit()
            print(f"Successfully inserted {len(df)} rows into employees_raw")
            
            # Close the connection
            cursor.close()
            conn.close()
            
    except Error as e:
        print(f"Error while connecting to MySQL: {e}")
    except Exception as e:
        print(f"Other error occurred: {e}")

# Function to clean data
def clean_data():
    cleaning_sql = """
    -- Clear the cleaned employees table
    TRUNCATE TABLE employees;
    
    -- Create a temporary table to handle duplicates
    DROP TABLE IF EXISTS temp_employees;
    CREATE TABLE temp_employees AS
    SELECT 
        -- Clean employee_id - convert to integer
        CAST(NULLIF(employee_id, '') AS UNSIGNED) AS employee_id,
        
        -- Clean name - handle empty values, remove titles, and trim extra spaces
        CASE 
            WHEN name = '' THEN 'Unknown' 
            ELSE TRIM(REGEXP_REPLACE(TRIM(name), '(Mr\\.|Mrs\\.|Ms\\.|Dr\\.|MD|PhD)', ''))
        END AS name,
        
        -- Clean age - convert to integer, handle non-numeric values
        CASE 
            WHEN age REGEXP '^[0-9]+$' THEN 
                CASE
                    WHEN CAST(age AS UNSIGNED) < 18 THEN NULL -- Flag unrealistic ages
                    WHEN CAST(age AS UNSIGNED) > 80 THEN NULL -- Flag unrealistic ages
                    ELSE CAST(age AS UNSIGNED)
                END
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
            WHEN LOWER(department) IN ('lgistics', 'logistics', 'logstics') THEN 'Logistics'
            WHEN LOWER(department) IN ('marketng', 'marketing') THEN 'Marketing'
            WHEN LOWER(department) IN ('sales', 'slaes') THEN 'Sales'
            WHEN LOWER(department) IN ('legal', 'legl') THEN 'Legal'
            ELSE department
        END AS department,
        
        -- Clean date_of_joining - convert various date formats to standard format
        -- Also handle future dates (using current date as the cutoff)
        CASE
            WHEN date_of_joining REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN 
                CASE 
                    WHEN STR_TO_DATE(date_of_joining, '%Y-%m-%d') > CURDATE() THEN NULL
                    ELSE STR_TO_DATE(date_of_joining, '%Y-%m-%d')
                END
            WHEN date_of_joining REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN 
                CASE 
                    WHEN STR_TO_DATE(date_of_joining, '%Y/%m/%d') > CURDATE() THEN NULL
                    ELSE STR_TO_DATE(date_of_joining, '%Y/%m/%d')
                END
            ELSE NULL
        END AS date_of_joining,
        
        -- Clean years_of_experience - improved to handle more formats
        CASE 
            -- Try to extract numeric values from text formats
            WHEN TRIM(REGEXP_REPLACE(years_of_experience, '[^0-9]', '')) != '' THEN 
                CASE
                    -- If age is available, validate experience doesn't exceed age-18
                    WHEN age REGEXP '^[0-9]+$' AND CAST(TRIM(REGEXP_REPLACE(years_of_experience, '[^0-9]', '')) AS UNSIGNED) > (CAST(age AS UNSIGNED) - 18) 
                        THEN (CAST(age AS UNSIGNED) - 18)
                    -- Experience seems unreasonably high (more than 40 years)
                   WHEN CAST(TRIM(REGEXP_REPLACE(years_of_experience, '[^0-9]', '')) AS UNSIGNED) > 40 
                        THEN 40
                    -- Negative experience doesn't make sense
                    WHEN CAST(TRIM(REGEXP_REPLACE(years_of_experience, '[^0-9]', '')) AS UNSIGNED) < 0 
                        THEN 0
                    ELSE CAST(TRIM(REGEXP_REPLACE(years_of_experience, '[^0-9]', '')) AS UNSIGNED)
                END
            -- For completely empty values, set to NULL for proper tracking
            WHEN years_of_experience IS NULL OR TRIM(years_of_experience) = '' 
                THEN NULL
            ELSE NULL
        END AS years_of_experience,
        
        -- Clean country - standardize country names (case-insensitive) and replace 0 with o
        CASE 
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%glarastan%' THEN 'Glarastan'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%hesperia%' THEN 'Hesperia'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%vorastria%' THEN 'Vorastria'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%velronia%' THEN 'Velronia'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%mordalia%' THEN 'Mordalia'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%drivania%' THEN 'Drivania'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%tavlora%' THEN 'Tavlora'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%zorathia%' THEN 'Zorathia'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%xanthoria%' THEN 'Xanthoria'
            WHEN LOWER(REPLACE(country, '0', 'o')) LIKE '%luronia%' THEN 'Luronia'
            WHEN country = '' THEN 'Unknown'
            ELSE country
        END AS country,
        
        -- Clean salary - convert to decimal, handle non-numeric values
        -- Also handle outliers (extremely high or low values)
        CASE 
            WHEN salary REGEXP '^[0-9]+(\.[0-9]+)?$' THEN 
            CASE
                -- Flag extremely high salaries (> 1,000,000)
                WHEN CAST(salary AS DECIMAL(10,2)) > 1000000 THEN NULL
                -- Flag extremely low salaries (< 10,000) for full-time employees
                WHEN CAST(salary AS DECIMAL(10,2)) < 10000 THEN NULL
                -- Mark salary as NULL if it is 0
                WHEN CAST(salary AS DECIMAL(10,2)) = 0 THEN NULL
                ELSE CAST(salary AS DECIMAL(10,2))
            END
            ELSE NULL
        END AS salary,
        
        -- Clean performance_rating - standardize ratings and handle missing values
        CASE 
            WHEN LOWER(performance_rating) LIKE '%top%' THEN 'Top Performers'
            WHEN LOWER(performance_rating) LIKE '%high%' THEN 'Top Performers'
            WHEN LOWER(performance_rating) LIKE '%average%' THEN 'Average Performers'
            WHEN LOWER(performance_rating) LIKE '%low%' THEN 'Low Performers'
            WHEN LOWER(performance_rating) LIKE '%poor%' THEN 'Low Performers'
            WHEN performance_rating = '' THEN NULL
            ELSE performance_rating
        END AS performance_rating,
        
        -- Add row number to handle duplicates
        ROW_NUMBER() OVER (PARTITION BY CAST(NULLIF(employee_id, '') AS UNSIGNED) ORDER BY 
                           CASE 
                               WHEN date_of_joining REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' THEN STR_TO_DATE(date_of_joining, '%Y-%m-%d')
                               WHEN date_of_joining REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' THEN STR_TO_DATE(date_of_joining, '%Y/%m/%d')
                               ELSE NULL
                           END DESC) AS row_num
    FROM employees_raw
    WHERE employee_id IS NOT NULL AND employee_id != '';
    
    -- Insert only the first occurrence of each employee_id into the final table
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
        employee_id, 
        name, 
        age, 
        department, 
        date_of_joining, 
        years_of_experience, 
        country, 
        salary, 
        performance_rating
    FROM 
        temp_employees
    WHERE 
        row_num = 1;
        
    -- Track duplicates in quality issues table
    CREATE TABLE IF NOT EXISTS data_quality_issues (
        id INT AUTO_INCREMENT PRIMARY KEY,
        employee_id INT,
        issue_type VARCHAR(50),
        description VARCHAR(255),
        fixed BOOLEAN DEFAULT FALSE
    );
    
    -- Truncate previous quality issues
    TRUNCATE TABLE data_quality_issues;
    
    -- Add duplicate records to data quality issues
    INSERT INTO data_quality_issues (employee_id, issue_type, description)
    SELECT 
        employee_id,
        'Duplicate Record',
        CONCAT('Found ', COUNT(*) - 1, ' duplicate records for this employee ID')
    FROM 
        temp_employees
    GROUP BY 
        employee_id
    HAVING 
        COUNT(*) > 1;
    
    -- Track records with missing values
    INSERT INTO data_quality_issues (employee_id, issue_type, description)
    SELECT 
        employee_id, 
        'Missing Data', 
        CONCAT('Missing values in: ', 
            CASE WHEN name = 'Unknown' THEN 'Name, ' ELSE '' END,
            CASE WHEN age IS NULL THEN 'Age, ' ELSE '' END,
            CASE WHEN date_of_joining IS NULL THEN 'Date of Joining, ' ELSE '' END,
            CASE WHEN years_of_experience IS NULL THEN 'Years of Experience, ' ELSE '' END,
            CASE WHEN country = 'Unknown' THEN 'Country, ' ELSE '' END,
            CASE WHEN salary IS NULL THEN 'Salary, ' ELSE '' END,
            CASE WHEN performance_rating = 'Not Evaluated' THEN 'Performance Rating' ELSE '' END
        )
    FROM 
        employees
    WHERE 
        name = 'Unknown' OR 
        age IS NULL OR 
        date_of_joining IS NULL OR 
        years_of_experience IS NULL OR 
        country = 'Unknown' OR
        salary IS NULL OR
        performance_rating = 'Not Evaluated';
    
    -- Track records with suspicious salary values
    INSERT INTO data_quality_issues (employee_id, issue_type, description)
    SELECT 
        e.employee_id,
        'Suspicious Salary',
        CONCAT('Salary (', er.salary, ') outside normal range for department/experience')
    FROM 
        employees_raw er
    JOIN 
        employees e ON CAST(NULLIF(er.employee_id, '') AS UNSIGNED) = e.employee_id
    WHERE 
        er.salary REGEXP '^[0-9]+(\.[0-9]+)?$' AND
        (CAST(er.salary AS DECIMAL(10,2)) > 500000 OR
        CAST(er.salary AS DECIMAL(10,2)) < 30000);
    
    -- Track records with suspicious experience values
    INSERT INTO data_quality_issues (employee_id, issue_type, description)
    SELECT 
        e.employee_id,
        'Suspicious Experience',
        'Years of experience unrealistic compared to age'
    FROM 
        employees_raw er
    JOIN 
        employees e ON CAST(NULLIF(er.employee_id, '') AS UNSIGNED) = e.employee_id
    WHERE 
        er.years_of_experience REGEXP '^[0-9]+$' AND
        er.age REGEXP '^[0-9]+$' AND
        CAST(er.years_of_experience AS UNSIGNED) > (CAST(er.age AS UNSIGNED) - 18);
    
    -- Track records with future joining dates
    INSERT INTO data_quality_issues (employee_id, issue_type, description)
    SELECT 
        CAST(NULLIF(employee_id, '') AS UNSIGNED) as employee_id,
        'Future Date',
        'Date of joining is in the future'
    FROM 
        employees_raw
    WHERE 
        (date_of_joining REGEXP '^[0-9]{4}-[0-9]{2}-[0-9]{2}$' AND STR_TO_DATE(date_of_joining, '%Y-%m-%d') > CURDATE()) OR
        (date_of_joining REGEXP '^[0-9]{4}/[0-9]{2}/[0-9]{2}$' AND STR_TO_DATE(date_of_joining, '%Y/%m/%d') > CURDATE());
        
    -- Clean up temporary table
    DROP TABLE IF EXISTS temp_employees;
    """
    execute_sql(cleaning_sql)

# Function to create salary by department analysis
def create_salary_by_department():
    sql = """
    DROP TABLE IF EXISTS salary_by_department;
    CREATE TABLE salary_by_department AS
    SELECT 
        Department,
        AVG(salary) AS AverageSalary,
        MIN(salary) AS MinimumSalary,
        MAX(salary) AS MaximumSalary
    FROM 
        employees
    WHERE 
        Department IS NOT NULL AND salary IS NOT NULL
    GROUP BY 
        Department
    ORDER BY 
        AverageSalary DESC;
    """
    execute_sql(sql)

# Function to create salary by experience analysis
def create_salary_by_experience():
    sql = """
    DROP TABLE IF EXISTS salary_by_experience;
    CREATE TABLE salary_by_experience AS
    SELECT 
        years_of_experience,
        AVG(salary) AS AverageSalary
    FROM 
        employees
    WHERE 
        years_of_experience IS NOT NULL AND salary IS NOT NULL
    GROUP BY 
        years_of_experience
    ORDER BY 
        years_of_experience ASC;
    """
    execute_sql(sql)

# Function to create performance analysis
def create_performance_analysis():
    sql = """
    DROP TABLE IF EXISTS performance_analysis;
    CREATE TABLE performance_analysis AS
    SELECT 
        performance_rating,
        AVG(salary) AS AverageSalary,
        COUNT(*) AS EmployeeCount
    FROM 
        employees
    WHERE 
        performance_rating IS NOT NULL AND salary IS NOT NULL
    GROUP BY 
        performance_rating
    ORDER BY 
        AverageSalary DESC;
    """
    execute_sql(sql)

# Function to create performance by start year analysis
def create_performance_by_start_year():
    sql = """
    DROP TABLE IF EXISTS performance_by_start_year;
    CREATE TABLE performance_by_start_year AS
    SELECT 
        YEAR(date_of_joining) AS StartYear,
        AVG(CASE 
            WHEN performance_rating = 'Top Performers' THEN 5
            WHEN performance_rating = 'High Performers' THEN 4
            WHEN performance_rating = 'Average Performers' THEN 3
            WHEN performance_rating = 'Low Performers' THEN 2
            WHEN performance_rating = 'Poor Performers' THEN 1
            ELSE NULL
        END) AS AveragePerformanceScore
    FROM 
        employees
    WHERE 
        date_of_joining IS NOT NULL AND performance_rating IS NOT NULL
    GROUP BY 
        YEAR(date_of_joining)
    ORDER BY 
        StartYear;
    """
    execute_sql(sql)

# Function to create salary by start year analysis
def create_salary_by_start_year():
    sql = """
    DROP TABLE IF EXISTS salary_by_start_year;
    CREATE TABLE salary_by_start_year AS
    SELECT 
        YEAR(date_of_joining) AS start_year,
        AVG(salary) AS avg_salary,
        MIN(salary) AS min_salary,
        MAX(salary) AS max_salary,
        COUNT(*) AS employee_count
    FROM 
        employees
    WHERE
        date_of_joining IS NOT NULL AND salary IS NOT NULL
    GROUP BY 
        YEAR(date_of_joining)
    ORDER BY 
        start_year;
    """
    execute_sql(sql)

# Function to create employee hire by start year analysis
def create_employee_hire_by_start_year():
    sql = """
    DROP TABLE IF EXISTS employee_hire_by_start_year;
    CREATE TABLE employee_hire_by_start_year AS
    SELECT 
        YEAR(date_of_joining) AS start_year,
        COUNT(employee_id) AS hire_count
    FROM 
        employees
    WHERE
        date_of_joining IS NOT NULL AND employee_id IS NOT NULL
    GROUP BY 
        YEAR(date_of_joining)
    ORDER BY 
        start_year;
    """
    execute_sql(sql)

# Function to create analysis by start year
def create_analysis_by_start_year():
    sql = """
    DROP TABLE IF EXISTS analysis_by_start_year;
    CREATE TABLE analysis_by_start_year AS
    SELECT 
        YEAR(date_of_joining) AS StartYear,
        COUNT(*) AS EmployeeCount,
        AVG(salary) AS AverageSalary,
        MIN(salary) AS MinimumSalary,
        MAX(salary) AS MaximumSalary,
        AVG(CASE 
            WHEN performance_rating = 'Top Performers' THEN 5
            WHEN performance_rating = 'High Performers' THEN 4
            WHEN performance_rating = 'Average Performers' THEN 3
            WHEN performance_rating = 'Low Performers' THEN 2
            WHEN performance_rating = 'Poor Performers' THEN 1
            ELSE NULL
        END) AS AveragePerformanceScore
    FROM 
        employees
    WHERE 
        date_of_joining IS NOT NULL AND salary IS NOT NULL AND performance_rating IS NOT NULL
    GROUP BY 
        YEAR(date_of_joining)
    ORDER BY 
        StartYear;
    """
    execute_sql(sql)


default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email': ['admin@example.com'],
    'email_on_failure': True,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'employee_data_pipeline',
    default_args=default_args,
    description='A pipeline to process employee data',
    schedule_interval='@daily',
    catchup=False
)

# Task 1: Data Ingestion
ingestion_task = PythonOperator(
    task_id='data_ingestion',
    python_callable=ingest_data_to_mysql,
    dag=dag,
)

# Task 2: Data Cleaning
cleaning_task = PythonOperator(
    task_id='data_cleaning',
    python_callable=clean_data,
    dag=dag,
)

# Task 3: Create Salary by Department Analysis
salary_by_department_task = PythonOperator(
    task_id='salary_by_department',
    python_callable=create_salary_by_department,
    dag=dag,
)

# Task 4: Create Salary by Experience Analysis
salary_by_experience_task = PythonOperator(
    task_id='salary_by_experience',
    python_callable=create_salary_by_experience,
    dag=dag,
)

# Task 5: Create Performance Rating Analysis
performance_analysis_task = PythonOperator(
    task_id='performance_analysis',
    python_callable=create_performance_analysis,
    dag=dag,
)

# Task 6: Create Performance by Start Year Analysis
performance_by_start_year_task = PythonOperator(
    task_id='performance_by_start_year',
    python_callable=create_performance_by_start_year,
    dag=dag,
)

# Task 7: Create Salary by Start Year Analysis
salary_by_start_year_task = PythonOperator(
    task_id='salary_by_start_year',
    python_callable=create_salary_by_start_year,
    dag=dag,
)

# Task 8: Create Employee Hire by Start Year Analysis
employee_hire_by_start_year_task = PythonOperator(
    task_id='employee_hire_by_start_year',
    python_callable=create_employee_hire_by_start_year,
    dag=dag,
)

# Task 9: Create Comprehensive Analysis by Start Year
analysis_by_start_year_task = PythonOperator(
    task_id='analysis_by_start_year',
    python_callable=create_analysis_by_start_year,
    dag=dag,
)

# Update dependencies
ingestion_task >> cleaning_task >> [salary_by_department_task, salary_by_experience_task, 
                performance_analysis_task, performance_by_start_year_task, 
                salary_by_start_year_task, employee_hire_by_start_year_task,
                analysis_by_start_year_task]
EOF

# Install required packages for the Python functions
pip install mysql-connector-python pandas

# Restart any running Airflow processes
pkill -f airflow

# Start Airflow scheduler and webserver
echo "Starting Airflow services..."
airflow scheduler -D
airflow webserver -p 8080 -D

echo "Setup completed successfully!"
echo "Airflow UI is available at http://localhost:8080"
echo "Username: admin"
echo "Password: admin"