"""
stage1_extract_clean.py

ETL #1 in the dependency chain: Extract -> Clean -> Load.

Reads the raw employee CSV (e.g. produced by generate_employee_data.py),
drops rows missing required fields, enforces correct dtypes, and writes a
cleaned copy to the staging area. This staged file is what ETL #2 depends on.

Run standalone for testing:
    python stage1_extract_clean.py \
        --input_path ../data/raw/employee_data.csv \
        --output_path ../data/staging/stage1_output.csv

"""

import argparse

from etl_common import extract, load, add_processed_timestamp

REQUIRED_COLUMNS = ["employee_id", "name", "address", "salary", "age"]


def transform(df):
    # Drop rows missing any required field
    df = df.dropna(subset=REQUIRED_COLUMNS, how="any")

    # Drop exact duplicate rows (defensive - upstream systems are messy in real life)
    df = df.drop_duplicates()

    # Enforce types so downstream stages never have to guess
    df["employee_id"] = df["employee_id"].astype(int)
    df["salary"] = df["salary"].astype(float)
    df["age"] = df["age"].astype(int)
    df["name"] = df["name"].astype(str).str.strip()
    df["address"] = df["address"].astype(str).str.strip()

    # Basic sanity filter: negative or zero salaries are bad data, not real employees
    df = df[df["salary"] > 0]

    df = add_processed_timestamp(df, column_name="stage1_processed_at")
    return df


def main():
    parser = argparse.ArgumentParser(description="Stage 1: extract + clean employee data")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    args = parser.parse_args()

    df_raw = extract(args.input_path)
    df_clean = transform(df_raw)
    load(df_clean, args.output_path, mode="overwrite")

    print(f"[stage1] read {len(df_raw)} rows, wrote {len(df_clean)} clean rows -> {args.output_path}")


if __name__ == "__main__":
    main()
