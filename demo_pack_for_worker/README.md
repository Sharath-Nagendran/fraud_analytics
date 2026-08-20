# Fraud Analytics Demo Pack

All data is fully synthetic and deterministic (seed 42). No real banking or personal data is included.

## Suggested platform flow
1. NiFi ingests CSV files and the JSONL stream sample into Raw/Landing.
2. Kafka carries transaction events; Kafka Connect routes events to storage/serving.
3. Airflow orchestrates validation, standardization, enrichment and gold-table publication.
4. Spark performs cleansing, joins, feature engineering and batch scoring.
5. Flink performs streaming velocity, device and geolocation checks.
6. Iceberg stores processed/curated transaction, alert and case tables with versioning.
7. ClickHouse serves low-latency Superset dashboards.
8. Atlas/DataHub catalogs datasets and lineage; Ranger applies masking/RBAC.

## Recommended ingestion order
customers -> accounts -> devices -> merchants -> transactions -> fraud_events -> alerts -> cases -> gold_daily_channel_city

## Demo storyline
Start with normal transactions, switch to the live risk dashboard, inject the JSONL events, show velocity/device/geographic anomalies, trace a flagged transaction through alert and case tables, then conclude on executive loss and operations dashboards.
