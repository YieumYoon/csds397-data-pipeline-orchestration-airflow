-- Create project_db database for our project
CREATE DATABASE IF NOT EXISTS project_db;

-- Grant privileges to airflow user for project_db database
GRANT ALL PRIVILEGES ON project_db.* TO 'airflow'@'%';
FLUSH PRIVILEGES;

-- Switch to project_db database
USE project_db;

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
);

-- Create tables for transformed data
CREATE TABLE IF NOT EXISTS department_analysis (
    department VARCHAR(50) PRIMARY KEY,
    employee_count INT,
    avg_salary DECIMAL(10,2),
    avg_age DECIMAL(5,2),
    avg_experience DECIMAL(5,2)
);

CREATE TABLE IF NOT EXISTS performance_analysis (
    performance_category VARCHAR(50) PRIMARY KEY,
    employee_count INT,
    avg_salary DECIMAL(10,2),
    max_salary DECIMAL(10,2),
    min_salary DECIMAL(10,2)
);

CREATE TABLE IF NOT EXISTS country_salary_report (
    country VARCHAR(50) PRIMARY KEY,
    employee_count INT,
    avg_salary DECIMAL(10,2),
    max_salary DECIMAL(10,2),
    min_salary DECIMAL(10,2),
    salary_range DECIMAL(10,2)
);