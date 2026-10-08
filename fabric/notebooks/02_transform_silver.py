"""Stage 2: Transform Bronze data into curated Silver data."""

BRONZE_TABLE = "bronze_raw_data"
SILVER_TABLE = "silver_clean_data"

print(f"Loading Bronze table: {BRONZE_TABLE}")
print("Applying cleansing, type casting, and basic quality rules")
print(f"Writing transformed dataset to Silver table: {SILVER_TABLE}")
