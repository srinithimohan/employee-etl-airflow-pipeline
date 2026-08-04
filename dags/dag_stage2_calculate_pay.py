"""
dag_stage2_calculate_pay.py

ETL #2 (middle of the chain).

`schedule=[STAGE1_OUTPUT_ASSET]` means this DAG has NO cron schedule of its
own - Airflow automatically queues a run of this DAG the moment DAG #1's
task finishes updating that asset. This is the literal implementation of
"the input of ETL 2 is the output of ETL 1" from the meeting.

The business-rule numbers (vacation policy) are pulled from Airflow
Variables at task *run* time via Jinja templates (`{{ var.value... }}`),
not hardcoded here and not baked in at parse time. That means someone can
go to Admin > Variables in the Airflow UI and change the vacation policy
without touching any code or redeploying anything - true "plug and play".
"""

from __future__ import annotations

import os
from datetime import datetime

from airflow.sdk import DAG, Asset
from airflow.providers.standard.operators.bash import BashOperator

DATA_DIR = os.environ["EMPLOYEE_ETL_DATA_DIR"]
CODE_DIR = os.environ["EMPLOYEE_ETL_CODE_DIR"]

# Same URI as declared in dag_stage1_extract_clean.py - Airflow matches
# assets by URI across files, so this doesn't need to be the same Python
# object, just the same string.
STAGE1_OUTPUT_ASSET = Asset(f"file://{DATA_DIR}/staging/stage1_output.csv")
STAGE2_OUTPUT_ASSET = Asset(f"file://{DATA_DIR}/staging/stage2_output.csv")

with DAG(
    dag_id="employee_etl_stage2_calculate_pay",
    description="ETL #2: calculate monthly salary + vacation days (auto-triggered by stage 1)",
    schedule=[STAGE1_OUTPUT_ASSET],
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["employee-etl", "stage2"],
) as dag:

    run_stage2 = BashOperator(
        task_id="run_stage2_calculate_pay",
        bash_command=(
            "python3 {{ params.code_dir }}/stage2_calculate_pay.py "
            "--input_path {{ params.data_dir }}/staging/stage1_output.csv "
            "--output_path {{ params.data_dir }}/staging/stage2_output.csv "
            "--base_vacation_days {{ var.value.get('BASE_VACATION_DAYS', 10) }} "
            "--years_per_bonus_day {{ var.value.get('YEARS_PER_BONUS_DAY', 5) }} "
            "--bonus_age_start {{ var.value.get('BONUS_AGE_START', 25) }} "
            "--max_vacation_days {{ var.value.get('MAX_VACATION_DAYS', 25) }}"
        ),
        params={"data_dir": DATA_DIR, "code_dir": CODE_DIR},
        outlets=[STAGE2_OUTPUT_ASSET],
    )
