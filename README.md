# 📱 PhonePe Transaction Insights
### Unified Analytics Dashboard | GUVI × HCL

> A single Streamlit application combining a **9-page general analytics dashboard** and **5 Business Case Study pages**, powered by SQLAlchemy + PostgreSQL + Plotly.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Database Schema](#database-schema)
- [Setup & Installation](#setup--installation)
- [Configuration](#configuration)
- [Running the ETL Pipeline](#running-the-etl-pipeline)
- [Running the Dashboard](#running-the-dashboard)
- [Dashboard Pages](#dashboard-pages)
- [Business Case Studies](#business-case-studies)
- [SQL Queries](#sql-queries)
- [Key Findings](#key-findings)

---

## 🎯 Project Overview

PhonePe is one of India's leading digital payments platforms. This project performs an end-to-end analysis of the [PhonePe Pulse](https://github.com/PhonePe/pulse) public dataset — covering transactions, user engagement, insurance, and geographic performance across all Indian states and districts.

**Domain:** Finance / Payment Systems  
**Skills:** Python · SQL · Streamlit · Data Visualization · ETL · PostgreSQL

---

## 🗂 Project Structure

```
phonepe-insights/
│
├── phonepe_app.py               # Unified dashboard — 9 general pages + 5 BCS pages
├── extract_load.py              # ETL pipeline — clone PhonePe Pulse, load to PostgreSQL
│
├── bcs_sql_queries.sql          # 24 SQL queries across the 5 chosen Business Cases
├── sql_queries.sql              # 10 additional analytical SQL queries
│
├── PhonePe_BCS_Documentation.docx   # BCS analysis process, visualizations & insights
├── PhonePe_Documentation.docx       # Full technical documentation
│
├── PhonePe_BCS_Presentation.pptx    # 18-slide BCS findings deck
├── PhonePe_Presentation.pptx        # 14-slide general project overview
│
└── README.md                    # This file
```

---

## 🛠 Tech Stack

| Layer | Technology |
|-------|-----------|
| Data Source | [PhonePe Pulse GitHub](https://github.com/PhonePe/pulse) — JSON files |
| ETL | Python 3.x · `git` · `json` · `sqlalchemy` · `psycopg2-binary` · `pandas` |
| Storage | PostgreSQL 13+ |
| ORM | SQLAlchemy 2.x — `declarative_base`, `Base.metadata.create_all` |
| SQL Analytics | PostgreSQL — CTEs, Window Functions (`LAG`, `RANK`, `FIRST_VALUE`) |
| Dashboard | Streamlit · Plotly Express · Plotly Graph Objects |
| Visualizations | 15+ chart types — line, bar, choropleth, heatmap, scatter, treemap, funnel, pie, dual-axis |
| Caching | `@st.cache_resource` (DB engine) · `@st.cache_data` (data loaders & generators) |

---

## 🗄 Database Schema

Nine tables across three tiers, all **lowercase** (PostgreSQL convention):

```
Aggregated Tier               Map Tier                  Top Tier
──────────────────────        ──────────────────────    ──────────────────────
aggregated_transaction        map_transaction           top_transaction
aggregated_user               map_user                  top_user
aggregated_insurance          map_insurance             top_insurance
```

**Key columns per tier:**

| Tier | Shared Columns | Extra Columns |
|------|---------------|---------------|
| Aggregated | `state`, `year`, `quarter` | transaction: `type`, `count`, `amount` · user: `brand`, `user_count`, `registered_users`, `app_opens` · insurance: `count`, `amount` |
| Map | `state`, `district`, `year`, `quarter` | same value columns as Aggregated |
| Top | `state`, `district`, `pincode`, `year`, `quarter` | same value columns as Aggregated |

> **Note:** PostgreSQL stores unquoted identifiers in lowercase. Always use lowercase table names in SQL queries.

---

## ⚙️ Setup & Installation

### Prerequisites

- Python 3.8+
- PostgreSQL 13+
- Git

### 1. Clone this repository

```bash
git clone <your-repo-url>
cd phonepe-insights
```

### 2. Create and activate a virtual environment

```bash
python -m venv .venv

# Windows
.venv\Scripts\activate

# macOS / Linux
source .venv/bin/activate
```

### 3. Install dependencies

```bash
pip install streamlit plotly pandas numpy sqlalchemy psycopg2-binary requests
```

---

## 🔧 Configuration

### Dashboard — `phonepe_app.py`

Update the database config block near the top of the file:

```python
# DATABASE CONFIG  — update these before running
DB_HOST     = "localhost"
DB_USER     = "postgres"
DB_PASSWORD = ""            # <-- your PostgreSQL password
DB_NAME     = "phonepe_pulse"
DB_PORT     = 5432
```

### ETL Pipeline — `extract_load.py`

Update the same variables at the top:

```python
DB_USER     = "root"
DB_PASSWORD = ""            # <-- your PostgreSQL password
DB_HOST     = "localhost"
DB_PORT     = 5432
DB_NAME     = "phonepe_pulse"
```

---

## 🔄 Running the ETL Pipeline

```bash
python extract_load.py
```

**What it does, step by step:**

1. Clones `https://github.com/PhonePe/pulse.git` into `./pulse/` (or pulls if it already exists)
2. Connects to the `postgres` default database and creates `phonepe_pulse` if it does not exist
3. Creates all 9 tables using SQLAlchemy ORM (`Base.metadata.create_all`)
4. Parses all JSON files and bulk-loads using `DataFrame.to_sql(method='multi', chunksize=500)`

**Expected runtime:** 5–15 minutes depending on internet speed.

> **Note:** PostgreSQL `CREATE DATABASE` requires `AUTOCOMMIT` mode and does not support `IF NOT EXISTS`. The script handles this with a `try/except` guard.

---

## 🚀 Running the Dashboard

```bash
streamlit run phonepe_app.py
```

Opens at `http://localhost:8501`

The app connects to PostgreSQL on startup. If the connection fails, it **automatically falls back to seeded demo data** (15 states, 5 years, all quarters) so the dashboard is always usable.

---

## 📊 Dashboard Pages

Select from the **📊 General Dashboard** dropdown in the sidebar:

| Page | Charts & Features |
|------|------------------|
| 🏠 **Executive Overview** | 4 KPI metrics · Category pie · Transaction value bar · Key insights panel |
| 💳 **Transaction Analysis** | Tabs: By Category · Quarterly Breakdown · State Comparison |
| 👥 **User Engagement** | Tabs: Device Brand Distribution · User Growth |
| 🗺️ **Geographical Insights** | India choropleth map (Transaction Amount / Count / User Count) · Ranked bar · State table · GeoJSON fallback bar chart |
| 🛡️ **Insurance Insights** | KPI metrics · Trend charts · State breakdown |
| 🔍 **Customer Segmentation** | State-level transaction volume and frequency quadrant |
| 📈 **Trend Analysis** | Tabs: Transaction Trends · YoY Growth |
| 🏆 **Top Performers** | Tabs: Top States · Top Transaction Types · Top Device Brands · Top-N slider |
| ⚠️ **Fraud Detection Signals** | Z-score anomaly detection · Flagged states table |

**Sidebar Filters** (applied to all general pages):
- 📅 Year(s) — multi-select
- 🏛️ State(s) — multi-select

---

## 📋 Business Case Studies

Select from the **📋 Business Case Studies** dropdown in the sidebar.
Selecting a business case takes priority over the general dashboard page.

| Case | Title | Key Analysis |
|------|-------|-------------|
| **BC1** | 📊 Decoding Transaction Dynamics | QoQ growth via `LAG()` · Category heatmap · State benchmarking · Decline detection |
| **BC2** | 📱 Device Dominance & User Engagement | Brand market share · Brand × State heatmap · Engagement ratio · Underutilisation quadrant |
| **BC3** | 🛡️ Insurance Penetration & Growth | Penetration rate · YoY growth · Opportunity score · Priority treemap |
| **BC7** | 🗺️ Transaction Analysis: States & Districts | State rankings · District drill-down · Pin code table · Quarter heatmap · HHI index |
| **BC8** | 👤 User Registration Analysis | Top states/districts/pin codes per quarter · QoQ growth heatmap · Growth hotspot detection |

Each BCS page includes:
- Scenario description header
- **🔍 SQL Query Used** expander with exact queries
- Tabbed charts (3–4 tabs per case)
- KPI cards
- **💡 Key Findings** section

**Additional BCS Sidebar Filter:**
- Quarter — primary quarter selector (used by BC8 snapshots)

---

## 🔍 SQL Queries

### `bcs_sql_queries.sql` — 24 queries across 5 Business Cases

| Business Case | Queries | Key SQL Technique |
|---------------|---------|------------------|
| BC1 – Transaction Dynamics | 4 | `LAG()` QoQ growth · `AVG() OVER()` benchmarking |
| BC2 – Device & Engagement | 5 | `SUM() OVER(PARTITION BY)` share % · Underutilised filter |
| BC3 – Insurance | 4 | Penetration rate `LEFT JOIN` · Opportunity score |
| BC7 – States & Districts | 6 | HHI concentration index · District rollup · QoQ `LAG()` |
| BC8 – User Registration | 5 | `FIRST_VALUE` / `LAST_VALUE` acceleration · Growth hotspot CTE |

### `sql_queries.sql` — 10 additional analytical queries

Customer Segmentation · Fraud Detection (z-score) · Geographical Insights · Payment Performance · User Engagement · Insurance Insights · Marketing Optimization · Trend Analysis · Top Performers · Competitive Benchmarking

---

## 💡 Key Findings

### BC1 — Transaction Dynamics
- **Peer-to-Peer** dominates volume (~45%) but **Financial Services** has the highest average ticket size
- **Merchant Payments** is the fastest growing category (+18% QoQ)
- **Q4 seasonal spike** across all categories — launch campaigns in Q3 to capture peak

### BC2 — Device & Engagement
- **Samsung + Xiaomi** = 52% of users — Android-first UI optimisation is essential
- States with engagement ratio **below 2x** need re-activation campaigns
- **Underutilisation Quadrant** reveals device-specific UX friction points

### BC3 — Insurance Penetration
- Average penetration **below 2%** — massive untapped cross-sell opportunity
- Insurance growing at **+23% YoY** — fastest-growing revenue segment
- **Critical Priority states** have large user base + near-zero penetration

### BC7 — States & Districts
- **Top 3 states** account for 40%+ of national transaction value
- **HHI analysis** shows highly concentrated states where tier-2 investment drives growth
- **Q4 seasonal spike** confirmed across the state × quarter heatmap

### BC8 — User Registration
- **Q1 and Q3** show highest registration QoQ growth — best acquisition windows
- **Pin-code data** exposes hyper-local growth clusters invisible at state level
- **Growth hotspot states** expected to cross into top tier within 2 quarters

---

## 📄 License

This project uses the [PhonePe Pulse data](https://github.com/PhonePe/pulse) licensed under [CC BY-SA 4.0](https://creativecommons.org/licenses/by-sa/4.0/).

---

*Built as part of the GUVI × HCL Data Analytics Project | Finance / Payment Systems Domain*
