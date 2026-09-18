# Contributing

## Prerequisites

- Databricks workspace with Serverless compute
- Unity Catalog: `airline_operations` catalog
- UC Volume: `airline_operations.bronze.data` with ANAC CSV files
- Git credential linked to this repository

## Local Development

1. Clone this repository into a Databricks Git folder
2. Ensure the `airline_operations` catalog and its schemas (`bronze`, `silver`, `gold`) exist
3. Place ANAC CSV files in `/Volumes/airline_operations/bronze/data/VRA/` and `/Volumes/airline_operations/bronze/data/references/`
4. Run notebooks in the order specified in the [Quick Start](README.md#quick-start) section

## Conventions

### Naming

- Notebooks: `{order}_{action}_{subject}.py` (e.g., `01_ingest_vra.py`)
- SQL files: `{order}_{table}_{action}.sql` (e.g., `02_vra_audited.sql`)
- Tables: `{schema}.{snake_case_name}` (e.g., `gold.obt_flights`)
- Tags: lowercase, snake_case keys and values

### Code Style

- Python: PEP 8, 4-space indentation, descriptive variable names
- SQL: lowercase keywords, CTEs for complex logic, table aliases
- Comments: Portuguese for UC column/table comments, English for code comments
- Each notebook starts with a markdown title cell describing its purpose

### Data Quality

- All expectations run in `WARN` mode (never `FAIL`) — silver must be a lossless mirror
- New expectations must be documented in the [Data Quality & Validation Strategy](README.md#data-quality--validation-strategy) section
- Quarantine view must be updated when new expectations are added

### Governance

- Every new table must have UC tags (`layer`, `domain`, `grain`, `source`)
- Every column must have a non-empty UC comment in Portuguese
- Gold tables must additionally have `pattern` and `consumption` tags
- Run `04_gold_governance.py` after any gold table change

## Pull Requests

1. Create a feature branch: `git checkout -b feat/description`
2. Make changes following the conventions above
3. Run the governance notebook to validate
4. Commit with a descriptive message (no prefixes like "senior", "restructure")
5. Push and open a PR
