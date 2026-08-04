"""
dag_stage3_finalize_load.py

ETL #3 (end of the chain) - "in the third ETL your actual job will be
executed", per the meeting notes.

Auto-triggered the moment ETL #2 finishes updating STAGE2_OUTPUT_ASSET.
Writes the finalized dataset and a summary report to data/final/ - a
different location than the staging files, as requested.
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.bash import BashOperator

DATA_DIR = os.environ["EMPLOYEE_ETL_DATA_DIR"]
CODE_DIR = os.environ["EMPLOYEE_ETL_CODE_DIR"]

STAGE2_OUTPUT_ASSET = Asset(f"file://{DATA_DIR}/staging/stage2_output.csv")
FINAL_OUTPUT_ASSET = Asset(f"file://{DATA_DIR}/final/employee_final.csv")

with DAG(
    dag_id="employee_etl_stage3_finalize_load",
    description="ETL #3: finalize + load the employee dataset and summary report",
    schedule=[STAGE2_OUTPUT_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["employee-etl", "stage3"],
) as dag:

    run_stage3 = BashOperator(
        task_id="run_stage3_finalize_load",
        bash_command=(
            "python3 {{ params.code_dir }}/stage3_finalize_load.py "
            "--input_path {{ params.data_dir }}/staging/stage2_output.csv "
            "--output_path {{ params.data_dir }}/final/employee_final.csv "
            "--summary_output_path {{ params.data_dir }}/final/salary_summary.csv"
        ),
        params={"data_dir": DATA_DIR, "code_dir": CODE_DIR},
        outlets=[FINAL_OUTPUT_ASSET],
    )
