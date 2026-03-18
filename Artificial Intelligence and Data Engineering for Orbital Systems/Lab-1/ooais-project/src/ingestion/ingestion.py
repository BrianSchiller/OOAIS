import csv
import os
import argparse
from collections import defaultdict

# --- CLI argument ---
parser = argparse.ArgumentParser(description="Process observations CSV")
parser.add_argument(
    "--temp",
    type=float,
    default=15.2,
    help="Temperature threshold (default: 15.2)"
)
args = parser.parse_args()
temp_threshold = args.temp

input_file = "data/raw/observations.csv"
clean_file = "data/processed/observations_clean.csv"
temp_file = "data/processed/observations_temp.csv"
reduced_file = "data/processed/observations_reduced.csv"

total_rows = 0
invalid_rows = 0
valid_rows = []
temp_rows = []
reduced_rows = []

unique_ids = set()
object_counts = defaultdict(int)
temperature_sum = 0.0

with open(input_file, newline='', encoding='utf-8') as csvfile:
    reader = csv.DictReader(csvfile)
    fieldnames = reader.fieldnames

    for row in reader:
        total_rows += 1
        unique_ids.add(row["object_id"])
        object_counts[row["object_id"]] += 1

        if (
            not row.get("object_id") or
            any(value is None or value.strip() == "" for value in row.values())
        ):
            invalid_rows += 1
            continue

        try:
            temp = float(row["temperature"])
        except ValueError:
            invalid_rows += 1
            continue

        valid_rows.append(row)

        temperature_sum += temp

        if temp > temp_threshold:
            temp_rows.append(row)

        reduced_rows.append({
            "timestamp": row["timestamp"],
            "object_id": row["object_id"],
            "temperature": row["temperature"]
        })

avg_temp = temperature_sum / len(valid_rows) if valid_rows else 0

with open(clean_file, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(valid_rows)

with open(temp_file, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=fieldnames)
    writer.writeheader()
    writer.writerows(temp_rows)

with open(reduced_file, "w", newline='', encoding='utf-8') as f:
    writer = csv.DictWriter(f, fieldnames=["timestamp", "object_id", "temperature"])
    writer.writeheader()
    writer.writerows(reduced_rows)

print("\n=== DATA SUMMARY ===")
print(f"Temperature threshold: {temp_threshold}")
print(f"Total rows: {total_rows}")
print(f"Valid rows: {len(valid_rows)}")
print(f"Invalid rows: {invalid_rows}")
print(f"Unique IDs: {len(unique_ids)}")

print("\n=== TEMPERATURE STATS ===")
print(f"Average temperature: {avg_temp:.2f}")
print(f"Rows with temperature > {temp_threshold}: {len(temp_rows)}")

print("\n=== OBJECT OCCURRENCES ===")
for obj_id, count in sorted(object_counts.items()):
    print(f"{obj_id}: {count}")