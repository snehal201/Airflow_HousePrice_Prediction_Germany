# Importing necessary functions from the sample module
from functions import (
    load_dataset,  # Function to load datasets from CSV or Excel files
    highest_roi,  # Function to calculate areas with the highest return on investment
    common_rental_price_range,  # Function to determine the most common rental price range
    top_rent_increases,  # Function to identify ZIP codes with the highest rent increases
    most_common_heating_type,  # Function to find the most commonly used heating type
    avg_rent_per_sqft_top_cities  # Function to calculate average rent per square foot in top cities
)

# Importing Airflow modules for DAG and PythonOperator
from airflow import DAG  # DAG (Directed Acyclic Graph) is a pipeline definition in Airflow
from airflow.operators.python import PythonOperator  # Allows running Python functions as tasks
# Importing datetime module for date and time handling
from datetime import datetime
# Importing pandas library for data manipulation and analysis
import pandas as pd


# Function to generate the rental property analysis report
def generate_report(**kwargs):
    """
    Generate a rental property analysis report.

    Parameters:
    kwargs: Additional arguments passed by Airflow

    Returns:
    None
    """
    data_dict = kwargs['ti'].xcom_pull(task_ids='load_and_process_data')
    print(data_dict)
    if data_dict is None:
        raise ValueError("No data was returned from task1.")
    
    # Convert the pulled data dictionary to a DataFrame
    data = pd.DataFrame.from_dict(data_dict)
    
    # Perform analysis using predefined functions
    roi = highest_roi(data)
    common_range = common_rental_price_range(data)
    rent_increases = top_rent_increases(data)
    heating_type = most_common_heating_type(data)
    rent_per_sqft = avg_rent_per_sqft_top_cities(data)

    # Create the report content
    report_content = f"""
    Rental Property Analysis Report
    ===============================

    1. Top 5 ROI Areas:
    {roi}
    2. Most Common Rental Price Range:
    {common_range}

    3. Top 10 ZIP Codes with Highest Rent Increases:
    {rent_increases}

    4. Most Common Heating Type:
    {heating_type}

    5. Average Rent Per Sqft in Top 5 Cities:
    {rent_per_sqft}
    """
    
    # Save the report to a file
    output_path = '/opt/airflow/dags/rental_property_report.txt'
    try:
        with open(output_path, 'w', encoding='utf-8', errors='replace') as report_file:
            report_file.write(report_content)
        print(f"Report saved successfully to {output_path}.")
    except Exception as e:
        raise Exception(f"Error while saving report: {e}")
    

# Default arguments for the DAG
default_args = {
    'owner': 'airflow',  # Owner of the DAG
    'start_date': datetime(2025, 1, 10),  # Start date of the DAG
    'retries': 1,  # Number of retry attempts in case of failure
}

# Define the DAG
dag = DAG(
    'rental_property_analysis',  # Unique name for the DAG
    default_args=default_args,
    description='Analyze rental property data',  # Short description of the DAG
    schedule=None,  # Manual trigger (no schedule)
    catchup=False,  # Do not run past dates
)


# Define the task to load and process data
task1 = PythonOperator(
    task_id='load_and_process_data',  # Unique task ID
    python_callable=load_dataset,  # Function to execute
    op_kwargs={'file_path': "/opt/airflow/dags/preproced_data.csv"},  # Arguments to the function
    dag=dag,
)

# Define the task to generate the report
task2 = PythonOperator(
    task_id='generate_report',  # Unique task ID
    python_callable=generate_report,  # Function to execute
    dag=dag,
)

# Set task dependencies
task1 >> task2  # Task1 must be completed before Task2