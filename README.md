# Synevyr Finance Lakehouse

A Databricks medallion Lakehouse pipeline built on the TPC-H sample dataset, implementing Bronze → Silver → Gold layers with data quality validation, cross-layer reconciliation, monitoring, and an AI/BI dashboard for Finance business analytics.

## Presentation link

You can view the presentation [here](https://canva.link/t69eyb92a2o3z1r) ander .ucu.edu.ua account.

## Data Source

All eight TPC-H tables from the Databricks sample dataset:

```
samples.tpch
```

| Table | Rows | Key |
|---|---:|---|
| customer | 750,000 | c_custkey |
| orders | 7,500,000 | o_orderkey |
| lineitem | 29,999,795 | (l_orderkey, l_linenumber) |
| supplier | 50,000 | s_suppkey |
| part | 1,000,000 | p_partkey |
| partsupp | 4,000,000 | (ps_partkey, ps_suppkey) |
| nation | 25 | n_nationkey |
| region | 5 | r_regionkey |

Orders span 1992-01-01 through 1998-08-02.

## Project Structure

```
Synevyr_finance_lakehouse/
├── src/
│   ├── config.py                           # Central configuration (catalog, schema, table prefixes)
│   └── synevyr_finance_lakehouse/          # Python package
│       ├── __init__.py                     # Package init
│       ├── bronze.py                       # Bronze ingestion functions
│       ├── silver.py                       # Silver transformation functions
│       ├── gold.py                         # Gold build functions (4 analytical views)
│       └── validation.py                   # Data quality validation functions
├── notebooks/
│   ├── 01_source_profiling                 # Profiles source TPC-H data before ingestion
│   ├── 02_bronze_ingestion                 # Ingests source → Bronze Delta tables
│   ├── 03_silver_transformation             # Bronze → Silver with two-phase validation
│   ├── 04_gold_transformation              # Silver → Gold with Bronze/Silver/Gold reconciliation
│   ├── 05_finance_business_questions       # Answers 4 finance questions from Gold tables
│   └── 06_finance_monitoring_alerting       # Order-total reconciliation monitoring & alerting
├── visualizations/                         # Dashboard and visualization assets
└── .git/                                   # Git version control
```

## Configuration

All table naming is centralized in `src/config.py`:

```python
CATALOG = "workspace"
SCHEMA = "default"
SOURCE_SCHEMA = "samples.tpch"

BRONZE_PREFIX = "finance_bronze"
SILVER_PREFIX = "finance_silver"
GOLD_PREFIX = "finance_gold"
```

Tables are named using `project_table(prefix, table)` which resolves to `{CATALOG}.{SCHEMA}.{prefix}_{table}` (e.g., `workspace.default.finance_gold_customer_revenue`).

## Medallion Architecture

### Bronze Layer

Raw ingestion with no transformation. Adds only `_ingested_at` and `_source_table` metadata columns.

**Notebook:** `02_bronze_ingestion`

Tables: `workspace.default.finance_bronze_{table}` for all 8 TPC-H tables.

### Silver Layer

Clean, validated tables with Bronze metadata removed. Preserves the normalized TPC-H entity structure.

**Notebook:** `03_silver_transformation`

Two-phase validation:
1. **Table-local**: key uniqueness, non-null checks, discount/tax ranges (0.00–0.10 / 0.00–0.08), positivity checks, market-segment domain
2. **Cross-table**: referential integrity (8 FK checks) and order-total reconciliation (TPC-H cent-level truncation, $0.01 tolerance)

Tables: `workspace.default.finance_silver_{table}` for all 8 TPC-H tables.

### Gold Layer

Pre-aggregated Finance analytical views built from validated Silver tables.

**Notebook:** `04_gold_transformation`

| Gold Table | Columns | Rows | Purpose |
|---|---|---:|---|
| finance_gold_customer_revenue | c_custkey, c_name, c_mktsegment, total_net_revenue, total_orders, avg_order_value | 750,000 | Customer-level revenue metrics |
| finance_gold_yearly_revenue | year, gross_revenue, net_revenue_discount, net_revenue_discount_tax | 7 | Yearly revenue by 3 calculation methods |
| finance_gold_supplier_performance | s_suppkey, s_name, supplier_net_revenue, avg_discount | 50,000 | Supplier revenue and discount metrics |
| finance_gold_segment_yearly_order_value | year, c_mktsegment, avg_order_value, order_count | 35 | Yearly avg order value by market segment |

Cross-layer reconciliation confirms Bronze, Silver, and Gold net revenue match within $0.01.

## Business Questions

**Notebook:** `05_finance_business_questions`
**Dashboard:** Revenue & Customer Analysis Dashboard

1. **Top 10 customers by net revenue** — Customer name, net revenue, and share of total revenue
2. **1997 revenue breakdown** — Gross extended price, net of discount, net of discount and tax
3. **Market segment with highest average order value** — Tracked year-over-year across 5 segments
4. **Top 5 suppliers by revenue** — Supplier name, net revenue, and average discount

The recommended reporting metric is **Net Revenue** (extended price after discount, excluding tax).

## Monitoring & Alerting

**Notebook:** `06_finance_monitoring_alerting`

Implements TPC-H order-total reconciliation against Silver tables, aggregates monthly monitoring trends, writes results to a Delta monitoring table, and evaluates alert rules. Includes an anomaly simulation that verifies the alerting logic catches injected discrepancies.

## Team

Synevyr team:

- Tarnavskyi Maksym-Vasyl: Source profiling, Bronze Layer, Silver Layer
- Stetsuk Kostiantyn: Monitoring
- Kornetskyi Yaroslav: Vizualizations
- Sampara Sofiia: Gold Layer, Questions
