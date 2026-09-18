# ANAC Flight Analytics Platform

A medallion-architecture lakehouse for Brazilian civil aviation data, built on Databricks and designed for AI-driven consumption. Ingests 12 months of ANAC's *Voo Regular Ativo* (VRA) dataset alongside reference registries (aerodromes, airlines, operation codes), transforms them through bronze → silver → gold layers, and exposes a denormalized One Big Table (OBT) optimized for natural-language querying via Genie Agent.

---

## Table of Contents

- [Architecture Overview](#architecture-overview)
- [Repository Structure](#repository-structure)
- [Architectural Decisions](#architectural-decisions)
- [Data Quality & Validation Strategy](#data-quality--validation-strategy)
- [Performance & Scalability](#performance--scalability)
- [Lineage & Governance](#lineage--governance)
- [Use Cases & Query Patterns](#use-cases--query-patterns)
- [Consumption Patterns](#consumption-patterns)
- [Trade-offs](#trade-offs)
- [Data Sources](#data-sources)

---

## Architecture Overview

```
┌─────────────────────────────────────────────────────────────────────────┐
│                         Unity Catalog: airline_operations                │
│                                                                         │
│  ┌───────────┐     ┌──────────────────┐     ┌────────────────────────┐  │
│  │  BRONZE   │     │      SILVER      │     │         GOLD           │  │
│  │           │     │                  │     │                        │  │
│  │  Raw CSV  │────▶│  Typed + cleaned  │────▶│  fact_flights          │  │
│  │  All str  │     │  Delta tables    │     │  dim_airport           │  │
│  │  No trans │     │  + quarantine    │     │  obt_flights (denorm)  │  │
│  └───────────┘     └──────────────────┘     └────────────────────────┘  │
│                                                                         │
│  Volume ────────────────────────────────────────────────▶ Genie Agent   │
└─────────────────────────────────────────────────────────────────────────┘
```

**Three-layer medallion architecture** with strict separation of concerns:

| Layer | Purpose | Format | Row Count |
|-------|---------|--------|-----------|
| **Bronze** | Raw ingestion, zero transformation, all-string schema | Delta (full refresh) | 1,014,705 (VRA) |
| **Silver** | Type casting, column renaming, enrichment flags, quarantine | Delta (CREATE OR REPLACE) | 1,014,705 (VRA) |
| **Gold** | Star schema (fact + dimension) + denormalized OBT for AI consumption | Delta (CREATE OR REPLACE) | 1,014,664 (OBT) |

---

## Repository Structure

```
analytics-lakehouse/
├── src/
│   ├── bronze/
│   │   ├── 01_ingest_vra.py              # VRA CSV → bronze.vra (full refresh)
│   │   └── 02_ingest_reference_data.py   # Aerodromes, airlines, op codes → bronze.*
│   ├── silver/
│   │   ├── 01_silver_mirror.py           # Bronze → silver (typed, enriched, renamed)
│   │   └── transformations/
│   │       ├── 01_vra_marked.sql          # Enrichment flags (ANAC registry joins)
│   │       ├── 02_vra_audited.sql         # Data contract: 9 expectations (warn mode)
│   │       └── 03_vra_quarantine.sql       # Diagnostic quarantine view
│   └── gold/
│       ├── 01_gold_obt_flights.py         # One Big Table (denormalized, 39 cols)
│       ├── 02_gold_dim_airport.py         # Dimension: airports (unified origin/dest)
│       ├── 03_gold_fact_flights.py        # Fact: flight steps with resolved FKs
│       └── 04_gold_governance.py          # Column comments, UC tags, validation
├── docs/
│   └── data_catalog.py                    # Notebook-format data dictionary
├── .gitignore
└── README.md
```

---

## Architectural Decisions

### 1. Warn Mode over Fail Mode in Data Quality

**Decision:** All nine SDP expectations in `02_vra_audited.sql` run in `WARN` mode, not `FAIL`.

**Rationale:** The silver layer functions as a lossless mirror of bronze — row counts must match exactly (1,014,705 → 1,014,705). Fail mode would drop rows that violate expectations, breaking this invariant and silently shrinking the dataset. Warn mode logs violations for observability while preserving every record. The quarantine view (`03_vra_quarantine.sql`) captures which rows violated which constraints, enabling downstream business decisions about exclusion at the gold layer — not the silver layer.

**Impact:** Silver.vra row count == bronze.vra row count, always. Quality issues are diagnosed, not hidden.

### 2. OBT (One Big Table) alongside Star Schema

**Decision:** Maintain both a normalized star schema (`fact_flights` + `dim_airport`) and a denormalized OBT (`obt_flights`).

**Rationale:** The star schema serves traditional BI consumption (Tableau, Power BI) where modelers expect separable dimensions. The OBT serves AI consumption via Genie Agent, where join-free access eliminates the need for an LLM to understand table relationships — every column the agent might need is in a single flat table. This dual-model approach costs ~15 MB of additional storage (0.03% of typical lakehouse capacity) while dramatically simplifying the consumption layer.

**Impact:** Genie Agent can answer any business question with a single-table `SELECT` — no `JOIN` clauses, no foreign-key reasoning, no schema discovery overhead.

### 3. Python for Ingestion, SQL for Transformation

**Decision:** Bronze ingestion uses PySpark; silver/gold transformations use SQL (with SDP declarations in pure SQL).

**Rationale:** Bronze ingestion requires file-system interaction (`spark.read.csv` with delimiter/encoding options, `_metadata` column extraction) that is more expressive in Python. Silver and gold layers are declarative set transformations — SQL is the natural language for this, and it makes the transformation logic readable, auditable, and portable. The SDP expectations in SQL are also reviewable by data stewards who may not know Python.

### 4. Full Refresh over Incremental (CDC)

**Decision:** All layers use `CREATE OR REPLACE` / `mode("overwrite")` rather than incremental merge.

**Rationale:** The dataset is ~1M rows and ~11–20 MB per layer. At this scale, full refresh completes in seconds and eliminates the complexity of change detection, deduplication, and merge conflicts. When the dataset grows beyond ~10M rows, the bronze layer can adopt Auto Loader with incremental merge; silver and gold can switch to `MERGE INTO` with minimal code changes.

---

## Data Quality & Validation Strategy

### Data Contract (Silver Layer)

Nine expectations defined in `02_vra_audited.sql`, all in **warn mode**:

| # | Expectation | Rule | Business Meaning |
|---|-------------|------|------------------|
| 1 | `horarios_previstos_presentes` | `scheduled_departure IS NOT NULL AND scheduled_arrival IS NOT NULL` | Scheduled times must exist |
| 2 | `situacao_voo_conhecida` | `flight_status IN ('REALIZADO', 'CANCELADO')` | Status must be a known value |
| 3 | `chegada_prevista_depois_da_partida_prevista` | `scheduled_arrival > scheduled_departure` | Arrival can't precede departure |
| 4 | `chegada_real_depois_da_partida_real` | `actual_arrival > actual_departure` | Same for actual times |
| 5 | `atraso_partida_plausivel` | `departure_delay_min BETWEEN -120 AND 1440` | Delays within plausible range |
| 6 | `atraso_chegada_plausivel` | `arrival_delay_min BETWEEN -120 AND 1440` | Same for arrivals |
| 7 | `empresa_no_cadastro_anac` | `empresa_no_cadastro = TRUE` | Airline exists in ANAC registry |
| 8 | `aeroporto_origem_no_cadastro_anac` | `origem_no_cadastro = TRUE` | Origin airport in ANAC registry |
| 9 | `aeroporto_destino_no_cadastro_anac` | `destino_no_cadastro = TRUE` | Destination airport in ANAC registry |

### Quarantine Strategy

`03_vra_quarantine.sql` creates a materialized view that mirrors every row that failed at least one expectation, annotated with a `motivos_quarentena` column containing the pipe-delimited list of violated constraint names. This is **diagnostic, not corrective** — silver.vra retains all rows; the decision to exclude specific categories is deferred to the gold layer and driven by business rules.

### Gold Layer Validation

`04_gold_governance.py` runs post-load validation queries:
- **Documentation coverage:** Every column in silver and gold must have a non-empty comment (currently 100% coverage, excluding system event-log tables).
- **Tag audit:** Every gold table carries five UC tags: `layer`, `domain`, `grain`, `pattern`, `consumption`.
- **Lineage check:** Queries `system.access.table_lineage` to verify the full bronze → silver → gold chain is intact.

---

## Performance & Scalability

### Current Data Volume

| Table | Rows | Size (MB) | Files | Columns |
|-------|-----:|----------:|------:|--------:|
| `bronze.vra` | 1,014,705 | 11.07 | 3 | 14 |
| `silver.vra` | 1,014,705 | 20.01 | 1 | 26 |
| `silver.aerodromes` | 496 | 0.02 | 1 | 13 |
| `silver.airlines` | 877 | 0.02 | 2 | 11 |
| `silver.operation_codes` | 13 | <0.01 | 1 | 4 |
| `gold.obt_flights` | 1,014,664 | 15.61 | 1 | 39 |
| `gold.fact_flights` | 1,014,664 | 15.28 | 1 | 31 |
| `gold.dim_airport` | 396 | 0.01 | 1 | 7 |

**Data window:** August 2025 – August 2026 (12 months, 366 distinct flight dates).

### Query Latency (OBT)

Aggregation queries against `gold.obt_flights` on serverless compute:

| Metric | Latency |
|--------|---------|
| P50 | ~1.0 s |
| P95 | ~8.5 s (includes cold-start) |
| Steady-state | <1.2 s |

*Measured with 5 consecutive `GROUP BY flight_status` aggregations on serverless SQL. Cold-start outlier (8.5 s) reflects cluster spin-up; steady-state queries are sub-second.*

### Storage Footprint

Total lakehouse footprint: **~62 MB** across 8 Delta tables. At 12 months of data, this projects to **~310 MB/year** — well within the range where full-refresh is optimal. The inflection point for incremental strategies is estimated at ~10M rows (~10 GB).

### Partitioning Strategy

No partitioning or liquid clustering is currently applied. At <1M rows and <16 MB per table, full scans are faster than partition pruning overhead. When the dataset exceeds ~5M rows, partitioning by `reference_month` (monthly partitions) or liquid clustering on `icao_airline` + `scheduled_departure_date` is the planned evolution.

### Pipeline End-to-End

Full bronze → silver → gold refresh completes in **under 2 minutes** on serverless compute, including governance and validation.

---

## Lineage & Governance

### Unity Catalog Tags

Every table across all layers carries standardized UC tags:

| Tag | Values | Purpose |
|-----|--------|---------|
| `layer` | `bronze`, `silver`, `gold` | Medallion positioning |
| `domain` | `aviation` | Business domain |
| `grain` | `flight_step`, `aerodrome`, `company`, `code`, `airport` | Row-level granularity |
| `source` | `ANAC-VRA`, `ANAC-Aerodromos`, `ANAC-Operador-Aereo`, `ANAC-seed` | Origin system |
| `pattern` | `fact`, `dimension`, `obt` | Modeling pattern |
| `consumption` | `bi`, `genie` | Target consumer |

### Lineage Tracking

Upstream/downstream dependencies are tracked via `system.access.table_lineage`:

```
Volume (CSV) ──▶ bronze.vra ──▶ silver.vra ──▶ gold.fact_flights ──▶ gold.obt_flights
                                          └─▶ gold.dim_airport  ──┘
                 bronze.aerodromes ──▶ silver.aerodromes ──┘
                 bronze.*_airlines ──▶ silver.airlines ──┘
                 bronze.codigos_operacao ──▶ silver.operation_codes ──┘
```

The governance notebook (`04_gold_governance.py`) queries this system table to verify the full chain is intact after each gold rebuild.

### Delta Versioning & Retention

All Delta tables have **deletion vectors enabled** and **column mapping mode** set to `name`, supporting:
- Time travel via `VERSION AS OF` and `TIMESTAMP AS OF`
- schema evolution without breaking downstream consumers
- ACID guarantees with optimistic concurrency control

Default Delta log retention (30 days) and deleted file retention (7 days) apply. No custom retention policy is configured given the dataset's size and refresh cadence.

### Column Documentation

100% of silver and gold columns have non-empty UC comments in Portuguese (for Genie Agent compatibility). Comments describe business semantics, not just column names — e.g., `airline_name` carries: *"Razão social da companhia aérea. Quando o código não existe no cadastro da ANAC, traz COMPANHIA NÃO CADASTRADA seguida do código, em vez de vazio."*

---

## Use Cases & Query Patterns

### Punctuality Analysis

```sql
SELECT
    airline_name,
    COUNT(*) AS flights,
    AVG(departure_delay_min) AS avg_dep_delay,
    AVG(arrival_delay_min) AS avg_arr_delay,
    SUM(CAST(departure_punctual AS INT)) * 100.0 / COUNT(*) AS dep_on_time_pct
FROM airline_operations.gold.obt_flights
WHERE flight_status = 'REALIZADO'
GROUP BY airline_name
ORDER BY dep_on_time_pct DESC;
```

### Route Performance

```sql
SELECT
    route_municipalities,
    COUNT(*) AS flights,
    AVG(minutes_recovered) AS avg_minutes_recovered
FROM airline_operations.gold.obt_flights
WHERE flight_status = 'REALIZADO'
GROUP BY route_municipalities
ORDER BY flights DESC
LIMIT 10;
```

### Cancellation Analysis

```sql
SELECT
    airline_name,
    origin_municipality,
    destination_municipality,
    COUNT(*) AS cancelled_flights
FROM airline_operations.gold.obt_flights
WHERE flight_cancelled = TRUE
GROUP BY airline_name, origin_municipality, destination_municipality
ORDER BY cancelled_flights DESC
LIMIT 10;
```

### Monthly Trend

```sql
SELECT
    reference_month,
    flight_status,
    COUNT(*) AS flights,
    AVG(arrival_delay_min) AS avg_delay
FROM airline_operations.gold.obt_flights
GROUP BY reference_month, flight_status
ORDER BY reference_month, flight_status;
```

---

## Consumption Patterns

### Genie Agent (NL2SQL)

The OBT (`gold.obt_flights`) is the primary table for Genie Agent. It is tagged `consumption = 'genie'` to signal that this is the AI-optimized surface. The table's design follows three principles for LLM-friendly schemas:

1. **Self-describing column names:** `airline_name`, `origin_municipality`, `departure_delay_min` — no abbreviations, no joins needed to resolve meaning.
2. **Pre-computed metrics:** `departure_punctual`, `arrival_punctual`, `minutes_recovered`, `delay_out_of_range` — the agent does not need to derive business logic; it filters and aggregates.
3. **Portuguese column comments:** Genie Agent reads UC comments to understand semantics. Every column carries a business definition in Portuguese, enabling the agent to map natural-language questions to the correct columns.

**Example queries the Genie Agent can generate:**

- *"Qual companhia aérea teve mais atrasos em São Paulo?"* → `WHERE origin_municipality LIKE '%São Paulo%' ... GROUP BY airline_name`
- *"Qual rota tem maior taxa de cancelamento?"* → `WHERE flight_cancelled = TRUE ... GROUP BY route_icao`
- *"Qual o tempo médio de recuperação de atrasos por mês?"* → `AVG(minutes_recovered) ... GROUP BY reference_month`

### Schema Constraints for LLM Reasoning

| Constraint | Implementation | Benefit |
|-----------|---------------|---------|
| Single-table access | OBT contains all needed columns | No JOIN reasoning required |
| No ambiguous synonyms | Each concept maps to exactly one column | Eliminates column-selection errors |
| Boolean flags pre-computed | `departure_punctual`, `flight_cancelled` | Agent avoids complex CASE logic |
| Human-readable values | `flight_status = 'REALIZADO'` not `1` | Agent can match natural language |
| Route as single column | `route_icao` (ICAO pair), `route_municipalities` | No need to concatenate origin + destination |

### BI Consumption

The star schema (`fact_flights` + `dim_airport`) supports traditional BI tools. `fact_flights` is tagged `consumption = 'bi'` and maintains normalized foreign keys for modelers who prefer star-join patterns.

---

## Trade-offs

### Python + SQL (not pure Python or pure SQL)

**Choice:** Python for bronze ingestion, SQL for silver/gold transformations.

**Cost:** Two skill sets required to maintain the pipeline.
**Benefit:** Each layer uses the most expressive tool. Python handles file I/O, metadata extraction, and conditional logic elegantly. SQL handles set-based transformations, window functions, and SDP expectations more readably. A pure-SQL approach would require complex CSV parsing workarounds; a pure-Python approach would bury transformation logic inside DataFrame API calls that are harder to audit.

### SDP Warn Mode Preserves the Silver Mirror

**Choice:** Warn mode in silver expectations instead of fail mode.

**Cost:** Quarantined rows remain in silver.vra — consumers must be aware that not all rows pass all quality checks.
**Benefit:** Silver is a true, lossless mirror of bronze. Row counts match exactly (1,014,705 → 1,014,705). The quarantine view provides full diagnostic visibility. Business rules about which categories to exclude are applied at gold, not silver — keeping silver as a reliable, auditable copy of raw data.

### Denormalization Cost vs Query Simplicity

**Choice:** OBT (39 columns, 15.61 MB) alongside star schema (31 + 7 columns, 15.29 MB).

**Cost:** ~15 MB of duplicate storage. Denormalized tables are harder to update when dimension attributes change (must rebuild OBT, not just dimension).
**Benefit:** Genie Agent and ad-hoc analysts query a single table — no joins, no schema discovery, no foreign-key reasoning. At 1M rows, the OBT rebuild takes seconds; the storage cost is negligible. The star schema remains available for BI tools that prefer normalized models.

### Full Refresh vs Incremental

**Choice:** `CREATE OR REPLACE` everywhere.

**Cost:** O(n) scan on every refresh. Not suitable for datasets >10M rows without modification.
**Benefit:** Idempotent, simple, no merge-conflict risk. At 1M rows, full refresh completes in under 2 minutes. The transition to incremental (Auto Loader + MERGE) is a planned, well-understood evolution — not an architectural rewrite.

---

## Data Sources

All data is public and sourced from **ANAC** (Agência Nacional de Aviação Civil), Brazil's civil aviation authority.

| Dataset | Description | File Pattern |
|---------|-------------|--------------|
| VRA | Voo Regular Ativo — scheduled flight operations | `VRA_YYYYM.csv` (monthly) |
| Aerodromes | Brazilian airport registry | `pda_aerodromos.csv` |
| National Airlines | National airline registry | `pda_empresas_aereas_nacionais.csv` |
| Foreign Airlines | Foreign airline registry | `pda_empresas_aereas_estrangeiros.csv` |
| Operation Codes | Flight type code reference | `pda_codigos_operacao.csv` |

Available at: [dados.gov.br](https://dados.gov.br).

---

*Built on Databricks Lakehouse | Unity Catalog | Delta Lake | Spark Declarative Pipelines | Genie Agent*
