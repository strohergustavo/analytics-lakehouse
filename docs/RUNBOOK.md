# Runbook — Troubleshooting Guide

## Pipeline Failure

### Symptom: Bronze ingestion fails with "Path does not exist"
**Cause:** ANAC CSV files not loaded into the UC volume.
**Fix:**
1. Check `/Volumes/airline_operations/bronze/data/VRA/` for VRA files
2. Check `/Volumes/airline_operations/bronze/data/references/` for reference files
3. Download missing files from [dados.gov.br](https://dados.gov.br)
4. Re-run `src/bronze/01_ingest_vra.py`

### Symptom: Silver row count != bronze row count
**Cause:** Unexpected filtering in the silver mirror SQL.
**Fix:**
1. Run `SELECT COUNT(*) FROM airline_operations.bronze.vra` and `SELECT COUNT(*) FROM airline_operations.silver.vra`
2. If different, check `01_silver_mirror.py` for accidental `WHERE` clauses
3. Silver must be a lossless mirror — re-run the notebook

### Symptom: Quarantine view shows 0 rows
**Cause:** SDP pipeline not run after silver rebuild.
**Fix:**
1. Run `src/silver/transformations/01_vra_marked.sql` first
2. Then `02_vra_audited.sql` (creates the expectation contract)
3. Then `03_vra_quarantine.sql` (materializes the quarantine view)
4. Verify: `SELECT COUNT(*) FROM airline_operations.silver.vra_quarentena` should return ~213K

### Symptom: Gold OBT has fewer rows than fact_flights
**Cause:** Dimension join dropped rows (should not happen — all joins are LEFT JOIN).
**Fix:**
1. Check `SELECT COUNT(*) FROM airline_operations.gold.fact_flights` vs `gold.obt_flights`
2. If different, verify dim_airport has all ICAO codes: `SELECT COUNT(DISTINCT icao_airport) FROM gold.dim_airport`
3. Re-run `01_gold_obt_flights.py`

### Symptom: Governance notebook reports missing column comments
**Cause:** New column added without a comment.
**Fix:**
1. Identify the column: `SELECT table_schema, table_name, column_name FROM airline_operations.information_schema.columns WHERE table_schema IN ('silver','gold') AND (comment IS NULL OR comment = '')`
2. Add a comment: `ALTER TABLE airline_operations.{schema}.{table} ALTER COLUMN {col} COMMENT '...'`
3. Re-run `04_gold_governance.py`

### Symptom: Genie Agent generates incorrect SQL
**Cause:** Missing or unclear column comments, or table not tagged `consumption = 'genie'`.
**Fix:**
1. Verify `gold.obt_flights` has tag `consumption = 'genie'`
2. Check all 39 columns have non-empty Portuguese comments
3. Test with a simple question: "Quantos voos foram cancelados?"
4. If still wrong, check if the Genie space is configured to use `airline_operations.gold` schema

## Rollback Strategy

### Roll back a table to a previous version
```sql
-- Find available versions
DESCRIBE HISTORY airline_operations.gold.obt_flights;

-- Roll back to version N
RESTORE TABLE airline_operations.gold.obt_flights TO VERSION AS OF N;
```

### Roll back the entire pipeline
1. Restore bronze tables to previous version (if needed)
2. Re-run silver and gold notebooks — they are idempotent and will rebuild from the restored bronze
3. Run governance notebook to validate

## Monitoring Endpoints

| Check | Query | Expected |
|-------|-------|----------|
| Bronze row count | `SELECT COUNT(*) FROM airline_operations.bronze.vra` | ~1,014,705 |
| Silver = Bronze | `SELECT (SELECT COUNT(*) FROM bronze.vra) - (SELECT COUNT(*) FROM silver.vra)` | 0 |
| Gold = Silver - dedup | `SELECT (SELECT COUNT(*) FROM silver.vra) - (SELECT COUNT(*) FROM gold.fact_flights)` | 41 |
| Quarantine exists | `SELECT COUNT(*) FROM silver.vra_quarentena` | ~213,545 |
| Comment coverage | `SELECT COUNT(*) FROM information_schema.columns WHERE table_schema IN ('silver','gold') AND (comment IS NULL OR comment = '')` | 0 |
| Tag coverage | `SELECT COUNT(*) FROM information_schema.tables WHERE table_schema = 'gold' AND table_name NOT IN (SELECT table_name FROM information_schema.table_tags WHERE tag_name = 'consumption')` | 0 |
