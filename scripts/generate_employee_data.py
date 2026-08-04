"""
Generates a synthetic employee dataset for ETL testing.

Fields: employee_id, name, address, salary, age

No paths, row counts, or ranges are hardcoded in the logic below —
everything that could change between runs is passed in as an argument,
so this script stays generic/plug-and-play as your ETL jobs scale up.

Usage:
    python generate_employee_data.py --num_records 5000 --output_path employee_data.csv
"""

import argparse
import csv
import random

from faker import Faker


def generate_employee_data(num_records, output_path, seed=None,
                            min_salary=35000, max_salary=180000,
                            min_age=21, max_age=65):
    fake = Faker()
    if seed is not None:
        Faker.seed(seed)
        random.seed(seed)

    with open(output_path, "w", newline="") as f:
        writer = csv.writer(f)
        writer.writerow(["employee_id", "name", "address", "salary", "age"])
        for employee_id in range(1, num_records + 1):
            name = fake.name()
            address = fake.address().replace("\n", ", ")
            salary = round(random.uniform(min_salary, max_salary), 2)
            age = random.randint(min_age, max_age)
            writer.writerow([employee_id, name, address, salary, age])


def main():
    parser = argparse.ArgumentParser(description="Generate synthetic employee data.")
    parser.add_argument("--num_records", type=int, default=5000,
                         help="Number of employee rows to generate.")
    parser.add_argument("--output_path", default="employee_data.csv",
                         help="Path to write the output CSV file.")
    parser.add_argument("--seed", type=int, default=None,
                         help="Random seed, for reproducible output.")
    parser.add_argument("--min_salary", type=float, default=35000)
    parser.add_argument("--max_salary", type=float, default=180000)
    parser.add_argument("--min_age", type=int, default=21)
    parser.add_argument("--max_age", type=int, default=65)
    args = parser.parse_args()

    generate_employee_data(
        num_records=args.num_records,
        output_path=args.output_path,
        seed=args.seed,
        min_salary=args.min_salary,
        max_salary=args.max_salary,
        min_age=args.min_age,
        max_age=args.max_age,
    )
    print(f"Generated {args.num_records} employee records -> {args.output_path}")


if __name__ == "__main__":
    main()
