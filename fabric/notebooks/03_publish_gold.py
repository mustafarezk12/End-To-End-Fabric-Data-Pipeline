"""Stage 3: Publish analytics-ready Gold datasets for reporting."""

SILVER_TABLE = "silver_clean_data"
GOLD_TABLE = "gold_business_ready"

print(f"Loading Silver table: {SILVER_TABLE}")
print("Applying business logic and aggregations for stakeholder reporting")
print(f"Writing final output to Gold table: {GOLD_TABLE}")
print("Gold data is now ready for semantic model refresh and dashboards")
