"""
stage3_finalize_load.py

  1. employee_final.csv   - the full, finalized per-employee dataset
  2. salary_summary.csv   - a small aggregate report (avg salary/vacation
                             days per age bracket)

Run standalone for testing:
    python stage3_finalize_load.py \
        --input_path ../data/staging/stage2_output.csv \
        --output_path ../data/final/employee_final.csv \
        --summary_output_path ../data/final/salary_summary.csv
"""

import argparse

import pandas as pd

from etl_common import extract, load, add_processed_timestamp


AGE_BRACKETS = [0, 25, 35, 45, 55, 65, 200]
AGE_BRACKET_LABELS = ["<25", "25-34", "35-44", "45-54", "55-64", "65+"]


def transform(df):
    df = df.copy()

    # Round money fields to 2 decimals for a clean final report
    df["salary"] = df["salary"].round(2)
    df["monthly_salary"] = df["monthly_salary"].round(2)

    df["age_bracket"] = pd.cut(
        df["age"], bins=AGE_BRACKETS, labels=AGE_BRACKET_LABELS, right=False
    )

    df = add_processed_timestamp(df, column_name="stage3_processed_at")
    return df


def build_summary(df):
    summary = (
        df.groupby("age_bracket", observed=True)
        .agg(
            employee_count=("employee_id", "count"),
            avg_salary=("salary", "mean"),
            avg_monthly_salary=("monthly_salary", "mean"),
            avg_vacation_days=("vacation_days", "mean"),
        )
        .reset_index()
    )
    summary["avg_salary"] = summary["avg_salary"].round(2)
    summary["avg_monthly_salary"] = summary["avg_monthly_salary"].round(2)
    summary["avg_vacation_days"] = summary["avg_vacation_days"].round(1)
    return summary


def main():
    parser = argparse.ArgumentParser(description="Stage 3: finalize + load final outputs")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--summary_output_path", required=True)
    args = parser.parse_args()

    df_in = extract(args.input_path)
    df_final = transform(df_in)
    summary = build_summary(df_final)

    load(df_final, args.output_path, mode="overwrite")
    load(summary, args.summary_output_path, mode="overwrite")

    print(f"[stage3] wrote {len(df_final)} final rows -> {args.output_path}")
    print(f"[stage3] wrote {len(summary)} summary rows -> {args.summary_output_path}")


if __name__ == "__main__":
    main()
