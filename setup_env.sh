#!/bin/bash
# setup_env.sh
#
# Source this file (don't just run it!) before starting Airflow, every
# time you open a new terminal:
#
#     source setup_env.sh
#
# This is the "no hardcoded paths" mechanism: the DAG files read these
# two variables from the OS environment instead of having any path typed
# into the Python code. Change PROJECT_DIR below to wherever you actually
# put this project on your machine - that's the ONLY place a path lives.

export PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

export EMPLOYEE_ETL_DATA_DIR="$PROJECT_DIR/data"
export EMPLOYEE_ETL_CODE_DIR="$PROJECT_DIR/etl"

# Keep Airflow's metadata DB/logs inside this project folder instead of ~/airflow
export AIRFLOW_HOME="$PROJECT_DIR/airflow_home"

# Point Airflow straight at THIS project's dags/ folder (env var override beats
# editing airflow.cfg by hand, and means you never copy/paste DAG files anywhere)
export AIRFLOW__CORE__DAGS_FOLDER="$PROJECT_DIR/dags"

echo "PROJECT_DIR            = $PROJECT_DIR"
echo "EMPLOYEE_ETL_DATA_DIR   = $EMPLOYEE_ETL_DATA_DIR"
echo "EMPLOYEE_ETL_CODE_DIR   = $EMPLOYEE_ETL_CODE_DIR"
echo "AIRFLOW_HOME            = $AIRFLOW_HOME"
echo "AIRFLOW dags_folder     = $AIRFLOW__CORE__DAGS_FOLDER"
