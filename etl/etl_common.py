"""
etl_common.py

Shared, generic helpers used by every stage of the pipeline.

Nothing in this file knows about "employee" data specifically, and nothing
in it hardcodes a path. Every stage script imports these functions and
supplies its own input/output paths as arguments (which, in turn, come from
Airflow Variables / DAG config - never typed literally into the code).
"""

from __future__ import annotations

import os
import sys
from datetime import datetime, timezone

import pandas as pd


def extract(source_path: str) -> pd.DataFrame:
    if not os.path.exists(source_path):
        raise FileNotFoundError(
            f"extract() could not find input file: {source_path}\n"
            f"Did an earlier stage run and write its output here yet?"
        )

    ext = os.path.splitext(source_path)[1].lower()
    if ext == ".csv":
        return pd.read_csv(source_path)
    elif ext in (".parquet", ".pq"):
        return pd.read_parquet(source_path)
    else:
        raise ValueError(f"Unsupported file extension for extract(): {ext}")


def load(df: pd.DataFrame, target_path: str, mode: str = "overwrite") -> None:

    target_dir = os.path.dirname(target_path)
    if target_dir:
        os.makedirs(target_dir, exist_ok=True)

    ext = os.path.splitext(target_path)[1].lower()

    if ext == ".csv":
        if mode == "append" and os.path.exists(target_path):
            df.to_csv(target_path, mode="a", header=False, index=False)
        else:
            df.to_csv(target_path, index=False)
    elif ext in (".parquet", ".pq"):
        if mode == "append":
            raise ValueError("append mode is not supported for parquet in this POC")
        df.to_parquet(target_path, index=False)
    else:
        raise ValueError(f"Unsupported file extension for load(): {ext}")


def add_processed_timestamp(df: pd.DataFrame, column_name: str = "etl_processed_at") -> pd.DataFrame:

    df = df.copy()
    df[column_name] = datetime.now(timezone.utc).isoformat()
    return df


def get_required_env(var_name: str) -> str:

    value = os.environ.get(var_name)
    if value is None or value == "":
        print(f"ERROR: required environment variable '{var_name}' is not set.", file=sys.stderr)
        sys.exit(1)
    return value
