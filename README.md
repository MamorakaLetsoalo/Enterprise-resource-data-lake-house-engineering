# Turning Fragmented Data into Decision-Ready Intelligence

> An end-to-end data platform built on **Databricks** and **PySpark** that transforms siloed ERP and CRM operational data into a trusted, scalable, insight-driven analytics layer — engineered for decision-making, not just storage.

---

## Tech Stack

![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=for-the-badge&logo=databricks&logoColor=white)
![Apache Spark](https://img.shields.io/badge/Apache%20Spark-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)
![PySpark](https://img.shields.io/badge/PySpark-3776AB?style=for-the-badge&logo=python&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-003366?style=for-the-badge&logo=databricks&logoColor=white)
![Azure](https://img.shields.io/badge/Azure%20Data%20Lake-0078D4?style=for-the-badge&logo=microsoft-azure&logoColor=white)
![Python](https://img.shields.io/badge/Python%203.10-FFD43B?style=for-the-badge&logo=python&logoColor=black)
![Git](https://img.shields.io/badge/Git-F05032?style=for-the-badge&logo=git&logoColor=white)

| Layer | Technology | Purpose |
|---|---|---|
| Ingestion | CSV / ERP & CRM exports → ADLS Gen2 | Raw source landing zone |
| Storage | Delta Lake on Azure Data Lake Storage | ACID-compliant versioned storage |
| Compute | Databricks (Apache Spark clusters) | Distributed ETL and transformation |
| Transformation | PySpark + Databricks Notebooks | Medallion pipeline logic |
| Modeling | Delta Live Tables / Star Schema | Analytics-ready Gold layer |
| Orchestration | Databricks Workflows (Jobs) | Scheduled pipeline execution |
| Visualization | Power BI (DirectQuery on Gold) | Business reporting |
| Version Control | Git / GitHub | Notebook and pipeline versioning |

---

## Problem Statement

Organizations commonly operate with disconnected data sources — ERP for operations, CRM for customers — with no single source of truth. The result:

- **Inconsistent entity definitions** — the word "customer" means something different in every system
- **Poor data quality** — missing values, duplicates, and format mismatches accumulate silently
- **Slow, unreliable reporting** — analysts spend more time cleaning data than generating insight
- **Low trust in data** — leadership makes decisions on gut feel because the numbers don't add up

This platform solves that — distributed, systematically, at every layer.

---

## Solution: Medallion Architecture (Bronze → Silver → Gold)

Data flows through three discrete, auditable Delta Lake layers — each with a clear contract and ownership boundary.

```
┌──────────────────────────────────────────────────────────────────────┐
│              MEDALLION ARCHITECTURE ON DATABRICKS                    │
├────────────────┬────────────────────┬────────────────────────────────┤
│  🟫 BRONZE     │  🩶 SILVER         │  🏆 GOLD                      │
│  Raw Ingestion │  Cleansed & Valid  │  Analytics-Ready               │
│                │                   │                                │
│  • ERP CSV     │  • Null handling   │  • Star schema (Delta)         │
│  • CRM CSV     │  • Deduplication   │  • Fact + dimension tables     │
│  • Immutable   │  • Type casting    │  • Optimized for Power BI      │
│  • Delta table │  • Standardization │  • Z-ORDER indexed             │
│  • Auto Loader │  • PySpark DQ rules│  • OPTIMIZE + VACUUM           │
└────────────────┴────────────────────┴────────────────────────────────┘
         ↓                   ↓                        ↓
   ADLS /bronze/       ADLS /silver/            ADLS /gold/
   Delta format        Delta format             Delta format
```

### Why Databricks + Delta Lake over a traditional SQL warehouse?

| Concern | Traditional SQL DW | Databricks + Delta Lake |
|---|---|---|
| Scale | Vertical scaling hits limits | Horizontally distributed Spark clusters |
| Schema evolution | Manual ALTER TABLE | Schema evolution built into Delta |
| Data versioning | No native history | Time travel — query any prior snapshot |
| Streaming + batch | Separate pipelines | Unified with Structured Streaming |
| Cost at scale | Expensive compute tiers | Autoscaling clusters, spot instances |
| Data quality | Post-load validation | Enforced at write time with Delta constraints |

---

## Key Engineering Decisions

### 1. Separation of Concerns with Delta Lake
Each layer is a separate Delta table with its own schema and write contract. This enables independent debugging, Silver layer reuse across teams, and Bronze as an immutable audit trail — all with full ACID guarantees and time travel.

### 2. Data Quality as a First-Class Concern
Quality is enforced at the Silver layer via PySpark transformations before any data reaches analytics. Embedded strategies:

- **Null handling** — explicit rules per column: coalesce defaults, flag unknowns, drop unfixable rows
- **Duplicate detection** — `dropDuplicates()` on composite business keys + window-based deduplication for late arrivals
- **Standardization** — consistent formats, casing, data types resolved across ERP and CRM schemas

### 3. Star Schema on Delta Lake
The Gold layer is structured as a fact-dimension model for:
- Minimal joins at query time for BI tools
- Fast aggregation on large transaction volumes
- Native Power BI DirectQuery support via Databricks SQL Warehouse

### 4. Performance-Oriented Gold Layer
- `OPTIMIZE` and `Z-ORDER BY` on high-cardinality join keys (`customer_id`, `product_id`, `sale_date`)
- `VACUUM` to manage Delta log file accumulation
- Partition pruning by `sale_year` / `sale_month` on `fact_sales`

---

## Data Model

```
                     
 ┌──────────────┐     ┌───────▼──────┐     ┌───────────────┐
 │ dim_customers├─────►  fact_sales  ◄─────┤  dim_products │
 └──────────────┘     └───────┬──────┘     └───────────────┘
                      
```

| Table | Type | Grain | Description |
|---|---|---|---|
| `gold.fact_sales` | Fact | One row per transaction line item | Core sales transactions |
| `gold.dim_customers` | Dimension | One row per customer | Cleansed customer master from CRM |
| `gold.dim_products` | Dimension | One row per product SKU | Standardized product catalog from ERP |

---

## Project Structure

```
data-platform-databricks/
├── notebooks/
│   ├── bronze/
│   │   └── ingest_raw_sources.py
│   ├── silver/
│   │   ├── clean_sales.py
│   │   ├── clean_customers.py
│   │   └── clean_products.py
│   ├── gold/
│   │   ├── build_dim_customers.py
│   │   ├── build_dim_products.py
│   │ 
│       └── build_fact_sales.py
│   └── analytics/
│       ├── top_customers.py
│       ├── product_revenue.py
│       └── sales_trends.py
├── workflows/
│   └── medallion_pipeline_job.json
├── tests/
│   ├── test_silver_quality.py
│   └── test_gold_schema.py
├── data/
│   ├── sample_erp_sales.csv
│   └── sample_crm_customers.csv
└── README.md
```

---

## Skills Demonstrated

### Engineering
| Skill | Description |
|---|---|
| Data Architecture Design | Medallion architecture on Delta Lake with ACID guarantees |
| Distributed ETL Development | PySpark pipelines with Auto Loader and Structured Streaming |
| Data Quality Engineering | Null handling, deduplication, and schema validation at Silver layer |
| Performance Engineering | Z-ORDER, OPTIMIZE, VACUUM, and partition pruning on Delta tables |

### Analytical
| Skill | Description |
|---|---|
| Business-to-Data Translation | Converted ambiguous business questions into precise PySpark queries |
| Problem Decomposition | Broke a complex data mess into a layered, auditable distributed solution |
| Insight Generation | CLV ranking, YoY product growth, and YTD revenue trend analysis |

### Technical
| Skill | Description |
|---|---|
| PySpark (Advanced) | Window functions, Delta merge, broadcast joins, streaming |
| Delta Lake | Time travel, schema evolution, OPTIMIZE, VACUUM, ACID transactions |
| Databricks Platform | Notebooks, Workflows, Unity Catalog, SQL Warehouse, Auto Loader |
| Star Schema Modeling | Fact-dimension design optimized for BI and aggregation workloads |

---

## Author

**Letsoalo M** — Business Data Analyst / Data Engineer

[![LinkedIn](https://img.shields.io/badge/LinkedIn-Connect-0A66C2?style=flat&logo=linkedin&logoColor=white)](https://linkedin.com)
[![GitHub](https://img.shields.io/badge/GitHub-Follow-181717?style=flat&logo=github&logoColor=white)](https://github.com)

---

*Built with PySpark · Databricks · Delta Lake · Medallion Architecture · Star Schema*
