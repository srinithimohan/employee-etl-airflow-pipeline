"""
stage2_calculate_pay.py

ETL #2 in the dependency chain: reads ETL #1's output (its input, by
definition, IS the previous stage's output - that's the "dependency tree"
from the meeting notes) and calculates two business figures per employee:

  - monthly_salary   = annual salary / 12
  - vacation_days

Run standalone for testing:
    python stage2_calculate_pay.py \
        --input_path ../data/staging/stage1_output.csv \
        --output_path ../data/staging/stage2_output.csv
"""

import argparse

from etl_common import extract, load, add_processed_timestamp


def calculate_vacation_days(age, base_days, years_per_bonus_day, bonus_age_start, max_days):
    if age <= bonus_age_start:
        days = base_days
    else:
        bonus_years = age - bonus_age_start
        bonus_days = bonus_years // years_per_bonus_day
        days = base_days + bonus_days
    return min(days, max_days)


def transform(df, base_vacation_days, years_per_bonus_day, bonus_age_start, max_vacation_days):
    df = df.copy()

    df["monthly_salary"] = (df["salary"] / 12).round(2)

    df["vacation_days"] = df["age"].apply(
        lambda age: calculate_vacation_days(
            age,
            base_days=base_vacation_days,
            years_per_bonus_day=years_per_bonus_day,
            bonus_age_start=bonus_age_start,
            max_days=max_vacation_days,
        )
    )

    df = add_processed_timestamp(df, column_name="stage2_processed_at")
    return df


def main():
    parser = argparse.ArgumentParser(description="Stage 2: calculate monthly salary + vacation days")
    parser.add_argument("--input_path", required=True)
    parser.add_argument("--output_path", required=True)
    parser.add_argument("--base_vacation_days", type=int, default=10,
                         help="Vacation days every employee starts with")
    parser.add_argument("--years_per_bonus_day", type=int, default=5,
                         help="Extra vacation day earned every N years of age past bonus_age_start")
    parser.add_argument("--bonus_age_start", type=int, default=25,
                         help="Age after which bonus vacation days start accruing")
    parser.add_argument("--max_vacation_days", type=int, default=25,
                         help="Hard cap on vacation days regardless of age")
    args = parser.parse_args()

    df_in = extract(args.input_path)
    df_out = transform(
        df_in,
        base_vacation_days=args.base_vacation_days,
        years_per_bonus_day=args.years_per_bonus_day,
        bonus_age_start=args.bonus_age_start,
        max_vacation_days=args.max_vacation_days,
    )
    load(df_out, args.output_path, mode="overwrite")

    print(f"[stage2] processed {len(df_out)} rows -> {args.output_path}")


if __name__ == "__main__":
    main()
