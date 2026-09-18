# Governance Policy

## Data Retention

| Component | Retention | Action |
|-----------|----------|--------|
| Delta log | 30 days (default) | Sufficient for rollback within a billing cycle |
| Deleted files | 7 days (default) | Allows VACUUM without breaking time travel |
| Bronze tables | Indefinite | Source of truth — never compacted or vacuumed |
| Silver tables | Indefinite | Rebuilt from bronze on each refresh |
| Gold tables | Indefinite | Rebuilt from silver on each refresh |
| UC Volume CSVs | Indefinite | ANAC public data — no expiry |
| Git history | Indefinite | Version-controlled via this repository |

## Access Control

| Principal | Bronze | Silver | Gold | Volume |
|-----------|--------|--------|------|--------|
| Data Engineer | `ALL` | `ALL` | `ALL` | `ALL` |
| Analyst | `SELECT` | `SELECT` | `SELECT` | — |
| Genie Agent | — | — | `SELECT` | — |
| Public (future) | — | — | `SELECT` (via share) | — |

*Note: Access control is managed via Unity Catalog grants. Current setup uses catalog-level `USE` and schema-level `SELECT` / `ALL` privileges.*

## Audit Trail

### Schema Changes
- Tracked via Delta transaction log (`DESCRIBE HISTORY`)
- Each `CREATE OR REPLACE TABLE` creates a new version
- Schema evolution is enabled (column mapping mode = name)

### Data Changes
- Tracked via Delta versions (time travel)
- Rollback via `RESTORE TABLE ... TO VERSION AS OF N`
- Current version counts: bronze.vra (16), silver.vra (28), gold.obt_flights (81)

### Access Audit
- Tracked via `system.access.audit` system table
- Lineage via `system.access.table_lineage`
- Query history via `system.access.table_lineage`

### Code Changes
- Tracked via Git (this repository)
- Branch-based development (see CONTRIBUTING.md)
- No direct commits to `main` in production

## SLA

| Metric | Target | Monitoring |
|--------|--------|------------|
| Pipeline completion time | < 5 minutes | Databricks job duration |
| Bronze to silver row count match | Exact (0 difference) | Governance notebook |
| Silver to gold row count difference | <= 41 (dedup only) | Governance notebook |
| Column comment coverage | 100% | Governance notebook |
| UC tag coverage | 100% | Governance notebook |
| Genie query latency P50 | < 2 seconds | Serverless compute metrics |
| Data freshness | Monthly (aligned with ANAC publication) | Manual / planned automation |

## Compliance

- **Data source:** ANAC public open data — no PII, no licensing restrictions
- **Storage region:** Databricks workspace region (configured at deployment)
- **Cross-border transfer:** Not applicable (Brazilian public data, stored in-region)
- **Data classification:** Public (suitable for open sharing via Delta Sharing)

## Change Management

1. All changes via pull request (see CONTRIBUTING.md)
2. Schema changes must preserve backward compatibility (column mapping mode = name)
3. New columns require UC comments in Portuguese
4. New tables require UC tags
5. Breaking changes require version bump and migration guide
