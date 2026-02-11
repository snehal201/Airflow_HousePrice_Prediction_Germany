# Importing necessary functions from the sample module
from functions import (
    load_dataset,  # Load datasets from CSV or Excel
    highest_roi,  # Calculate areas with highest ROI
    common_rental_price_range,  # Determine most common rental price range
    top_rent_increases,  # Identify ZIP codes with highest rent increases
    most_common_heating_type,  # Find most commonly used heating type
    avg_rent_per_sqft_top_cities  # Calculate avg rent per sqft in top cities
)

# Importing Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd


# Function to generate the rental property analysis report
def generate_report(**kwargs):
    """
    Generate a rental property analysis report.
    Pulls data from XCom, analyzes it, and saves a text report.
    """
    # Pull data from the previous task
    ti = kwargs['ti']
    data_payload = ti.xcom_pull(task_ids='load_and_process_data')

    if data_payload is None:
        raise ValueError("No data was returned from task 'load_and_process_data'.")

    # DEBUG: Check what type of data we received
    print(f"Data received type: {type(data_payload)}")

    # Handle Data Reconstruction
    # If using Pickle (default in older Airflow), data_payload might already be a DataFrame.
    # If using JSON (newer Airflow), it might be a dict or list.
    if isinstance(data_payload, pd.DataFrame):
        data = data_payload
    else:
        # Assuming it's a dict-like structure if not a DataFrame
        data = pd.DataFrame.from_dict(data_payload)

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
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 10),
    'retries': 1,
}

# Define the DAG
dag = DAG(
    'rental_property_analysis',
    default_args=default_args,
    description='Analyze rental property data',
    schedule_interval=None,  # 'schedule' is used in newer Airflow, 'schedule_interval' in older.
    catchup=False,
)

# Define the task to load and process data
task1 = PythonOperator(
    task_id='load_and_process_data',
    python_callable=load_dataset,
    # Ensure this path exists inside your Airflow container
    op_kwargs={'file_path': "/opt/airflow/dags/preproced_data.csv"},
    dag=dag,
)

# Define the task to generate the report
task2 = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# Set task dependencies
task1 >> task2