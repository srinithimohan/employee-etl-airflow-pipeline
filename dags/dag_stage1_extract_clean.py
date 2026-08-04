"""
dag_stage1_extract_clean.py

ETL #1 (root of the chain).

This is the only DAG in the chain that runs on a real time-based schedule
(or is triggered manually / by an external system). It cleans the raw
employee CSV and writes the result to the staging area as
`stage1_output.csv`. That file is declared as an Airflow "Asset" via
`outlets=`, which is what lets DAG #2 automatically watch for it.

Configuration:
    Reads EMPLOYEE_ETL_DATA_DIR and EMPLOYEE_ETL_CODE_DIR from the OS
    environment (set once before you start Airflow - see the setup guide).
    Nothing is hardcoded in this file.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.bash import BashOperator

DATA_DIR = os.environ["EMPLOYEE_ETL_DATA_DIR"]
CODE_DIR = os.environ["EMPLOYEE_ETL_CODE_DIR"]

# Declaring this as an Asset (not just a plain file path) is what lets
# DAG #2 schedule itself off of it - this is the mechanism Abhay described
# in the meeting as "ETL 2's input is ETL 1's output".
STAGE1_OUTPUT_ASSET = Asset(f"file://{DATA_DIR}/staging/stage1_output.csv")

with DAG(
    dag_id="employee_etl_stage1_extract_clean",
    description="ETL #1: extract raw employee CSV and clean it",
    schedule="@daily",       # change to None if you only want to trigger it by hand
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["employee-etl", "stage1"],
) as dag:


    """ 
    run_stage1 = BashOperator(
    task_id="run_stage1_extract_clean",
    bash_command=(
        "spark-submit {{ params.code_dir }}/etl_job.py "
        "--source_type csv --source_path {{ params.data_dir }}/raw/employee_data.csv "
        "--target_type csv --target_path {{ params.data_dir }}/staging/stage1_output.csv"
    ),
    params={"data_dir": DATA_DIR, "code_dir": CODE_DIR},
    outlets=[STAGE1_OUTPUT_ASSET],
)
    
    """
    run_stage1 = BashOperator(
        task_id="run_stage1_extract_clean",
        bash_command=(
            "python3 {{ params.code_dir }}/stage1_extract_clean.py "
            "--input_path {{ params.data_dir }}/raw/employee_data.csv "
            "--output_path {{ params.data_dir }}/staging/stage1_output.csv"
        ),
        params={"data_dir": DATA_DIR, "code_dir": CODE_DIR},
        outlets=[STAGE1_OUTPUT_ASSET],
    )
