# Silver Layer

Typed, enriched, and validated mirror of bronze. Silver preserves the bronze row count exactly — no filtering, no business rules. Data quality is observed via SDP expectations in warn mode, with a quarantine view for diagnostics.

## Tables

| Table | Source | Rows | Description |
|-------|--------|-----:|-------------|
| `silver.vra` | bronze.vra | 1,014,705 | Typed timestamps, delay arithmetic, enrichment flags |
| `silver.airlines` | bronze.national_airlines + foreign_airlines | 877 | Unified airline registry with `registry_origin` |
| `silver.aerodromes` | bronze.aerodromos | 496 | Airport reference with altitude cast to double |
| `silver.operation_codes` | bronze.operation_codes | 13 | DI and line-type code descriptions |
| `silver.vra_quarentena` | silver.vra (SDP) | 213,545 | Diagnostic quarantine — rows failing >=1 expectation |

## Notebooks

| File | Description |
|------|-------------|
| `01_silver_mirror.py` | Transforms bronze into typed silver tables, applies column comments and UC tags |
| `transformations/01_vra_marked.sql` | SDP: enrichment flags via LEFT JOINs to ANAC registries |
| `transformations/02_vra_audited.sql` | SDP: data contract with 9 expectations (all warn mode) |
| `transformations/03_vra_quarantine.sql` | SDP: materialized view of quarantined rows with violation reasons |

## Key Transformations

- `nullif(column, 'null')` before `try_cast` — ANAC encodes missing values as string `'null'`
- `timestampdiff(MINUTE, ...)` for delay arithmetic
- `minutes_recovered` = departure delay - arrival delay (can be negative)
- Enrichment flags: `empresa_no_cadastro`, `origem_no_cadastro`, `destino_no_cadastro`

## Data Quality Contract

9 expectations, all in warn mode. See [README.md](../../README.md#data-quality--validation-strategy) for the full contract.

## Dependencies

- Bronze layer must be complete (all 5 tables)
- SDP pipeline must run in order: mark -> audit -> quarantine
