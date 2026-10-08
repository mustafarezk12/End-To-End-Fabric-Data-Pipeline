"""Stage 1: Ingest raw data into the Bronze layer."""

# Fabric notebook template: replace placeholders with actual Fabric runtime objects.
SOURCE_PATH = "Files/raw/source_data"
BRONZE_TABLE = "bronze_raw_data"

print(f"Reading raw files from: {SOURCE_PATH}")
print(f"Writing ingested dataset to Bronze table: {BRONZE_TABLE}")
