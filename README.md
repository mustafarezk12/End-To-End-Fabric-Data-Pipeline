# End-To-End-Fabric-Data-Pipeline

This repository contains a Microsoft Fabric-oriented, end-to-end data pipeline blueprint that covers the full data lifecycle:

1. **Data ingestion** from source systems into a Fabric Lakehouse (Bronze layer)
2. **Data transformation** from Bronze to Silver to improve quality and structure
3. **Data load/publish** into a Gold layer for analytics consumption
4. **Visualization readiness** by preparing curated outputs for semantic models and BI dashboards
5. **Stakeholder sharing** through clear, trusted, and business-ready datasets

## Target architecture

- **Orchestration**: Fabric pipeline (`fabric/pipelines/e2e_pipeline.json`)
- **Transformations**: Notebook stages (`fabric/notebooks/*.py`)
- **Configuration**: Parameterized settings (`fabric/config/pipeline_config.yaml`)
- **Output**: Curated Gold tables ready for reporting and dashboarding

## Pipeline flow

1. `ingest_raw_to_bronze`
2. `transform_bronze_to_silver`
3. `transform_silver_to_gold`
4. `data_quality_checks`
5. `refresh_semantic_model`

## Repository structure

- `fabric/pipelines/e2e_pipeline.json` - pipeline orchestration definition
- `fabric/config/pipeline_config.yaml` - source/target/runtime parameters
- `fabric/notebooks/01_ingest_bronze.py` - ingestion logic template
- `fabric/notebooks/02_transform_silver.py` - transformation logic template
- `fabric/notebooks/03_publish_gold.py` - gold publish logic template

## Notes

This is a minimal scaffold designed to make the complete lifecycle explicit and implementation-ready in Fabric.
