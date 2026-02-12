# Importing Airflow modules
from airflow import DAG
from airflow.operators.python import PythonOperator
from datetime import datetime
import pandas as pd
import os

# Importing necessary functions from your custom module
# Ensure 'functions.py' is in the PYTHONPATH or the same directory
from functions import (
    load_dataset,  # Load datasets from CSV or Excel
    highest_roi,  # Calculate areas with highest ROI
    common_rental_price_range,  # Determine most common rental price range
    top_rent_increases,  # Identify ZIP codes with highest rent increases
    most_common_heating_type,  # Find most commonly used heating type
    avg_rent_per_sqft_top_cities  # Calculate avg rent per sqft in top cities
)

# --- Configuration ---
# specific paths to ensure Airflow finds files correctly
DATA_PATH = "/opt/airflow/dags/preproced_data.csv"  # Note: Check spelling of 'preproced' vs 'preprocessed'
REPORT_PATH = "/opt/airflow/dags/rental_property_report.txt"


# --- Functions ---

def generate_report(**kwargs):
    """
    Generate a rental property analysis report.
    Pulls data from XCom, analyzes it, and saves a text report.
    """
    # Pull data from the previous task using XCom
    ti = kwargs['ti']
    data_payload = ti.xcom_pull(task_ids='load_and_process_data')

    if data_payload is None:
        raise ValueError("No data was returned from task 'load_and_process_data'.")

    # --- Data Reconstruction Logic ---
    # Airflow XComs often serialize DataFrames as JSON (dicts) or Pickle.
    # This block handles both cases robustly.
    print(f"DEBUG: Data received type: {type(data_payload)}")

    if isinstance(data_payload, pd.DataFrame):
        data = data_payload
    elif isinstance(data_payload, (dict, list)):
        # If data was serialized to JSON/Dict
        data = pd.DataFrame.from_dict(data_payload)
    else:
        raise TypeError(f"Unsupported data type received from XCom: {type(data_payload)}")

    # Perform analysis using imported functions
    try:
        roi = highest_roi(data)
        common_range = common_rental_price_range(data)
        rent_increases = top_rent_increases(data)
        heating_type = most_common_heating_type(data)
        rent_per_sqft = avg_rent_per_sqft_top_cities(data)
    except Exception as e:
        raise RuntimeError(f"Error during analysis calculation: {e}")

    # Create the report content
    report_content = f"""
    Rental Property Analysis Report
    ===============================
    Generated on: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}

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
    try:
        # Ensure directory exists (optional safety)
        os.makedirs(os.path.dirname(REPORT_PATH), exist_ok=True)

        with open(REPORT_PATH, 'w', encoding='utf-8') as report_file:
            report_file.write(report_content)
        print(f"Report saved successfully to {REPORT_PATH}.")
    except Exception as e:
        raise IOError(f"Error while saving report to {REPORT_PATH}: {e}")


# --- DAG Definition ---

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 1, 10),
    'retries': 1,
}

dag = DAG(
    'rental_property_analysis',
    default_args=default_args,
    description='Analyze rental property data',
    # 'schedule' is for Airflow 2.4+. Use 'schedule_interval' if on older versions.
    schedule=None,
    catchup=False,
    tags=['analytics', 'rental'],
)

# --- Tasks ---

# Task 1: Load Data
# Note: Ensure load_dataset returns a DataFrame or Dict to be pushed to XCom automatically
task1 = PythonOperator(
    task_id='load_and_process_data',
    python_callable=load_dataset,
    op_kwargs={'file_path': DATA_PATH},
    dag=dag,
)

# Task 2: Generate Report
task2 = PythonOperator(
    task_id='generate_report',
    python_callable=generate_report,
    dag=dag,
)

# --- Dependencies ---
task1 >> task2