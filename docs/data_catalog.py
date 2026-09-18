# Databricks notebook source
# MAGIC %md
# MAGIC # Airline Operations — ANAC Flight Data Platform
# MAGIC
# MAGIC A medallion-architecture data platform built on Databricks Unity Catalog that ingests, transforms, and serves Brazilian civil aviation data (ANAC's *Voo Regular Ativo* dataset) for analytics and AI consumption.
# MAGIC
# MAGIC ## Architecture
# MAGIC
# MAGIC ```
# MAGIC ┌─────────────┐     ┌──────────────┐     ┌─────────────────────────────┐
# MAGIC │  CSV Files   │     │              │     │  Spark Declarative Pipeline  │
# MAGIC │  (UC Volume) │────▶│  Bronze Layer │────▶│  Data Quality + Quarantine   │
# MAGIC │              │     │  (raw, as-is) │     │  (expectations, warn mode)  │
# MAGIC └─────────────┘     └──────┬───────┘     └─────────────────────────────┘
# MAGIC                            │
# MAGIC                     ┌──────▼───────┐
# MAGIC                     │  Silver Layer │     Typed, documented, governed mirror
# MAGIC                     │  (typed)     │     No filtering, no business rules
# MAGIC                     └──────┬───────┘
# MAGIC                            │
# MAGIC                     ┌──────▼──────────────────────────────────┐
# MAGIC                     │            Gold Layer                    │
# MAGIC                     │  ┌─────────────┐  ┌──────────────┐      │
# MAGIC                     │  │ dim_airport  │  │ fact_flights │      │
# MAGIC                     │  └──────┬──────┘  └──────┬───────┘      │
# MAGIC                     │         │                │              │
# MAGIC                     │         └────┬───────────┘              │
# MAGIC                     │              ▼                          │
# MAGIC                     │        ┌──────────┐                    │
# MAGIC                     │        │obt_flights│  ← Genie / BI      │
# MAGIC                     │        └──────────┘                    │
# MAGIC                     └────────────────────────────────────────┘
# MAGIC ```
# MAGIC
# MAGIC ## Table Inventory
# MAGIC
# MAGIC | Layer | Table | Rows | Grain | Source |
# MAGIC | --- | --- | --- | --- | --- |
# MAGIC | Bronze | `bronze.vra` | 1,014,705 | flight step | ANAC CSV (12 months) |
# MAGIC | Bronze | `bronze.aerodromos` | 496 | aerodrome | ANAC AerodromosPublicos |
# MAGIC | Bronze | `bronze.national_airlines` | 729 | company | ANAC Empresas Nacionais |
# MAGIC | Bronze | `bronze.foreign_airlines` | 148 | company | ANAC Empresas Estrangeiras |
# MAGIC | Bronze | `bronze.operation_codes` | 13 | code | Curated seed (ANAC docs) |
# MAGIC | Silver | `silver.vra` | 1,014,705 | flight step | bronze.vra (1:1 mirror) |
# MAGIC | Silver | `silver.airlines` | 877 | company | national + foreign union |
# MAGIC | Silver | `silver.aerodromes` | 496 | aerodrome | bronze.aerodromos |
# MAGIC | Silver | `silver.operation_codes` | 13 | code | bronze.operation_codes |
# MAGIC | Silver | `silver.vra_auditado` | pipeline | flight step | Pipeline (9 expectations) |
# MAGIC | Silver | `silver.vra_quarentena` | pipeline | flight step | Pipeline (failed records) |
# MAGIC | Gold | `gold.dim_airport` | ~300 | airport | All ICAO codes from VRA |
# MAGIC | Gold | `gold.fact_flights` | 1,014,664 | flight step | silver.vra (deduped -41) |
# MAGIC | Gold | `gold.obt_flights` | 1,014,664 | flight step | fact + dim (denormalised) |
# MAGIC
# MAGIC ## Data Quality Pipeline
# MAGIC
# MAGIC A Spark Declarative Pipeline (`airline_operations_quality`) runs 9 expectations on `silver.vra` in **warn mode** (no rows dropped):
# MAGIC
# MAGIC 1. Scheduled departure and arrival present
# MAGIC 2. Flight status is REALIZADO or CANCELADO
# MAGIC 3. Scheduled arrival after scheduled departure
# MAGIC 4. Actual arrival after actual departure
# MAGIC 5. Departure delay within -120 to +1440 minutes
# MAGIC 6. Arrival delay within -120 to +1440 minutes
# MAGIC 7. Airline exists in ANAC registry
# MAGIC 8. Origin airport exists in ANAC registry
# MAGIC 9. Destination airport exists in ANAC registry
# MAGIC
# MAGIC Records failing any expectation land in `silver.vra_quarentena` with the reason(s) attached.
# MAGIC
# MAGIC ## Business Metrics
# MAGIC
# MAGIC * **Punctuality threshold**: 15 minutes (departure and arrival)
# MAGIC * **Delay range plausibility**: -2h to +24h (outside → metrics nullified, row kept)
# MAGIC * **Flight scope**: Domestic (N, C) vs International (I, G) derived from line type
# MAGIC * **Minutes recovered**: departure delay minus arrival delay (positive = recovered time in air)
# MAGIC * **Day of week**: Portuguese values (domingo–sabado) for Genie Agent compatibility
# MAGIC
# MAGIC ## Notebooks
# MAGIC
# MAGIC | Notebook | Layer | Purpose |
# MAGIC | --- | --- | --- |
# MAGIC | `Bronze_VRA` | Bronze | VRA CSV ingestion → Delta |
# MAGIC | `Bronze_REF` | Bronze | Reference tables (airports, airlines, codes) |
# MAGIC | `Silver_Mirror` | Silver | Typed mirror + documentation |
# MAGIC | `Gold_DIM` | Gold | Airport dimension |
# MAGIC | `Gold_FACT` | Gold | Flight fact table with business rules |
# MAGIC | `Gold_OBT` | Gold | One Big Table for AI/BI consumption |
# MAGIC | `Gold_Governance` | Gold | Comments, tags, and validation |
# MAGIC
# MAGIC ## Run Order
# MAGIC
# MAGIC 1. `Bronze_VRA` → `Bronze_REF` (ingestion)
# MAGIC 2. `Silver_Mirror` (transformation + documentation)
# MAGIC 3. Pipeline `airline_operations_quality` (data quality)
# MAGIC 4. `Gold_DIM` → `Gold_FACT` → `Gold_OBT` (dimensional model)
# MAGIC 5. `Gold_Governance` (metadata + validation)

# COMMAND ----------

# Full validation: row counts, documentation coverage, and tags across all layers
print("=" * 80)
print("AIRLINE OPERATIONS — VALIDATION REPORT")
print("=" * 80)

# 1. Row counts
print("\n--- ROW COUNTS ---")
for schema in ['bronze', 'silver', 'gold']:
    tables = spark.sql(f"SHOW TABLES IN airline_operations.{schema}").collect()
    for t in tables:
        full = f"airline_operations.{schema}.{t['tableName']}"
        try:
            cnt = spark.table(full).count()
            print(f"  {full}: {cnt:,} rows")
        except Exception as e:
            print(f"  {full}: (table not materialised yet)")

# 2. Column documentation coverage
print("\n--- DOCUMENTATION COVERAGE ---")
doc = spark.sql("""
    SELECT table_schema, table_name,
           COUNT(*) AS total_columns,
           SUM(CASE WHEN comment IS NULL OR comment = '' THEN 1 ELSE 0 END) AS without_comment
    FROM airline_operations.information_schema.columns
    WHERE table_schema IN ('bronze', 'silver', 'gold')
    GROUP BY table_schema, table_name
    ORDER BY table_schema, table_name
""").collect()
all_documented = True
for row in doc:
    status = "\u2705" if row['without_comment'] == 0 else "\u274c"
    if row['without_comment'] > 0:
        all_documented = False
    print(f"  {status} {row['table_schema']}.{row['table_name']}: {row['total_columns']} cols, {row['without_comment']} without comment")
print(f"\n  All columns documented: {all_documented}")

# 3. Tag coverage
print("\n--- TABLE TAGS ---")
tags = spark.sql("""
    SELECT table_schema, table_name, tag_name, tag_value
    FROM airline_operations.information_schema.table_tags
    ORDER BY table_schema, table_name, tag_name
""").collect()
for row in tags:
    print(f"  {row['table_schema']}.{row['table_name']}: {row['tag_name']} = {row['tag_value']}")

# 4. Bronze → Silver → Gold lineage check
print("\n--- LINEAGE CHECK ---")
bronze_vra = spark.table("airline_operations.bronze.vra").count()
silver_vra = spark.table("airline_operations.silver.vra").count()
gold_fact = spark.table("airline_operations.gold.fact_flights").count()
gold_obt = spark.table("airline_operations.gold.obt_flights").count()
print(f"  bronze.vra      → silver.vra:     {bronze_vra:,} → {silver_vra:,}  (diff: {bronze_vra - silver_vra:,})")
print(f"  silver.vra      → gold.fact:      {silver_vra:,} → {gold_fact:,}  (deduped: {silver_vra - gold_fact:,})")
print(f"  gold.fact       → gold.obt:       {gold_fact:,} → {gold_obt:,}  (diff: {gold_fact - gold_obt:,})")

print("\n" + "=" * 80)
print("VALIDATION COMPLETE")
print("=" * 80)