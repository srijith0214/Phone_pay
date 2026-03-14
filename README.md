# 📱 PhonePe Transaction Insights
### End-to-End Data Analytics Platform | GUVI × HCL

> Analyzing transaction dynamics, user engagement, insurance penetration, and geographic performance across India using the PhonePe Pulse public dataset.

---

## 📋 Table of Contents

- [Project Overview](#project-overview)
- [Business Case Studies](#business-case-studies)
- [Project Structure](#project-structure)
- [Tech Stack](#tech-stack)
- [Database Schema](#database-schema)
- [Setup & Installation](#setup--installation)
- [Running the ETL Pipeline](#running-the-etl-pipeline)
- [Running the Streamlit Dashboard](#running-the-streamlit-dashboard)
- [SQL Queries](#sql-queries)
- [Deliverables](#deliverables)
- [Key Findings](#key-findings)

---

## 🎯 Project Overview

PhonePe is one of India's leading digital payments platforms. This project performs an end-to-end analysis of the [PhonePe Pulse](https://github.com/PhonePe/pulse) public dataset — covering transactions, user engagement, insurance, and geographic performance across all Indian states and districts.

**Domain:** Finance / Payment Systems  
**Timeline:** 14 Days  
**Skills:** Python · SQL · Streamlit · Data Visualization · ETL

---

## 📊 Business Case Studies

Five business cases were selected and implemented with dedicated SQL queries, dashboard pages, and documented insights:

| # | Business Case | Key Question |
|---|---------------|-------------|
| **BC1** | Decoding Transaction Dynamics | Which categories and states are growing, stagnating, or declining? |
| **BC2** | Device Dominance & User Engagement | Which brands dominate? Where are users registered but inactive? |
| **BC3** | Insurance Penetration & Growth | Which states have the lowest insurance adoption and highest potential? |
| **BC7** | Transaction Analysis: States & Districts | Which states, districts, and pin codes lead by volume and value? |
| **BC8** | User Registration Analysis | Which states, districts, and pin codes drive the most new registrations? |

---

## 🗂 Project Structure

```
phonepe-insights/
│
├── extract_load.py              # ETL pipeline — clone, parse, load to MySQL
├── bcs_streamlit_app.py         # Main dashboard — 5 Business Case Study pages
├── phonepe_app.py               # Extended dashboard — 9 analytical pages
│
├── bcs_sql_queries.sql          # 24 SQL queries across the 5 chosen BCS
├── sql_queries.sql              # 10 additional analytical SQL queries
│
├── PhonePe_BCS_Documentation.docx   # BCS analysis process, visualizations & insights
├── PhonePe_Documentation.docx       # Full technical documentation (architecture, schema, setup)
│
├── PhonePe_BCS_Presentation.pptx    # 18-slide deck — all 5 BCS findings
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
| Storage | PostgreSQL |
| SQL Analytics | MySQL — CTEs, Window Functions (LAG, RANK, FIRST_VALUE) |
| Dashboard | Streamlit · Plotly Express · Plotly Graph Objects |
| Visualization | 15+ chart types — line, bar, heatmap, scatter, treemap, funnel, pie, dual-axis |
| Documentation | Microsoft Word (.docx) |
| Presentation | Microsoft PowerPoint (.pptx) |

---

## 🗄 Database Schema

Nine tables across three tiers:

```
Aggregated Tier          Map Tier                 Top Tier
─────────────────        ────────────────────     ─────────────────────
Aggregated_transaction   Map_transaction          Top_transaction
Aggregated_user          Map_user                 Top_user
Aggregated_insurance     Map_insurance            Top_insurance
```

**Key columns:**
- All tables: `state`, `year`, `quarter`
- Transaction tables: `transaction_count`, `transaction_amount`, `transaction_type`
- User tables: `registered_users`, `app_opens`, `brand`
- Insurance tables: `insurance_count`, `insurance_amount`
- Map/Top tables: additional `district`, `pincode` columns

**Indexes:** Composite indexes on `(state, year, quarter)` for all tables.

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

### 2. Install Python dependencies

```bash
pip install streamlit plotly pandas numpy sqlalchemy psycopg2-binary
```

### 3. Configure database credentials

Open `extract_load.py` and update the connection variables at the top:

```python
DB_CONFIG = {
    "host":     "localhost",
    "user":     "root",
    "password": "YOUR_PASSWORD_HERE",   # ← update this
    "database": "phonepe_pulse"   # must already exist in PostgreSQL
}
```

---

## 🔄 Running the ETL Pipeline

The ETL script automatically clones the PhonePe Pulse repository and loads all data into MySQL.

```bash
python extract_load.py
```

**What it does:**
1. Clones `https://github.com/PhonePe/pulse.git` into `./pulse/` (or pulls if already exists)
2. Creates the `phonepe_pulse` database (if it doesn't exist) and all 9 tables
3. Parses JSON files from `pulse/data/aggregated/`, `pulse/data/map/`, `pulse/data/top/`
4. Loads all records using batch inserts with `ON DUPLICATE KEY UPDATE`

**Expected runtime:** 5–15 minutes depending on internet speed.

---

## 🚀 Running the Streamlit Dashboard

### 5 Business Case Study Dashboard (recommended)

```bash
streamlit run bcs_streamlit_app.py
```

Opens at `http://localhost:8501`

**Pages available (select from sidebar):**

| Page | Charts |
|------|--------|
| BC1 – Transaction Dynamics | QoQ trend lines · Heatmap · State bar · Volume vs value scatter |
| BC2 – Device & Engagement | Brand pie · Brand×State heatmap · Engagement ratio bar · Underutilisation quadrant |
| BC3 – Insurance Penetration | Penetration bar · Dual-axis growth · Users vs penetration scatter · Opportunity treemap |
| BC7 – States & Districts | State rank bar · District drill-down · Pin code tables · Quarter heatmap |
| BC8 – User Registration | State funnel · District table · Pin code table · Growth hotspot heatmap |

**Sidebar filters** apply globally across all pages:
- Year (multi-select)
- State (multi-select)
- Quarter (primary selector)

### Extended 9-Page Dashboard

```bash
streamlit run phonepe_app.py
```

Includes Executive Overview, Geographical Insights (choropleth), Customer Segmentation, Fraud Detection Signals, Trend Analysis, and more.

> **Note:** Both dashboards include demo data generators and work without a database connection — they fall back to seeded synthetic data automatically.

---

## 🔍 SQL Queries

### `bcs_sql_queries.sql` — 24 queries across 5 BCS

| Business Case | Queries | Key SQL Technique |
|---------------|---------|------------------|
| BC1 – Transaction Dynamics | 4 | `LAG()` for QoQ growth · State benchmarking with `AVG() OVER()` |
| BC2 – Device & Engagement | 5 | `SUM() OVER(PARTITION BY)` for share % · Underutilised brand filter |
| BC3 – Insurance | 4 | Penetration rate via `LEFT JOIN` · Opportunity score formula |
| BC7 – States & Districts | 6 | HHI concentration index · District rollup from `Map_transaction` |
| BC8 – User Registration | 5 | `FIRST_VALUE` / `LAST_VALUE` for acceleration · Growth hotspot CTE |

### `sql_queries.sql` — 10 additional analytical queries

Customer Segmentation · Fraud Detection (z-score) · Geographical Insights · Payment Performance · User Engagement · Insurance Insights · Marketing Optimization · Trend Analysis · Top Performers · Competitive Benchmarking

---

## 📦 Deliverables

| # | Deliverable | Files | Status |
|---|-------------|-------|--------|
| 1 | **Source Code** — ETL pipeline | `extract_load.py` | ✅ Complete |
| 1 | **Source Code** — SQL queries | `bcs_sql_queries.sql` · `sql_queries.sql` | ✅ Complete |
| 1 | **Source Code** — Streamlit app | `bcs_streamlit_app.py` · `phonepe_app.py` | ✅ Complete |
| 2 | **Documentation** — BCS analysis, visualizations, insights | `PhonePe_BCS_Documentation.docx` | ✅ Complete |
| 2 | **Documentation** — Architecture, schema, setup guide | `PhonePe_Documentation.docx` | ✅ Complete |
| 3 | **Presentation** — 5 BCS findings & recommendations | `PhonePe_BCS_Presentation.pptx` (18 slides) | ✅ Complete |
| 3 | **Presentation** — General project overview | `PhonePe_Presentation.pptx` (14 slides) | ✅ Complete |

---

## 💡 Key Findings

### BC1 — Transaction Dynamics
- **Peer-to-Peer** dominates volume (~45%) but **Financial Services** commands the highest average ticket size
- **Merchant Payments** shows the fastest QoQ growth (+18%) — top investment priority
- **Q4 seasonal spike** visible across all categories — launch campaigns in Q3 to capture peak

### BC2 — Device & Engagement
- **Samsung + Xiaomi** = 52% of users — Android-first UI optimisation is essential
- **4 states** have engagement ratios below 2x — re-activation push campaigns needed
- **Underutilisation Quadrant** reveals device-specific UX friction in high-share brands

### BC3 — Insurance Penetration
- Average penetration is **below 2%** of registered users — a massive untapped cross-sell market
- Insurance growing at **+23% YoY** — the fastest-growing revenue segment
- **Critical Priority states** identified with large user base + near-zero penetration

### BC7 — States & Districts
- **Top 3 states** (Maharashtra, Karnataka, Tamil Nadu) = 40%+ of national transaction value
- **Mumbai** is the single highest-value district nationally
- **HHI analysis** reveals several states are highly concentrated — tier-2 city development is a direct growth lever

### BC8 — User Registration
- **Q1 and Q3** show the highest registration QoQ growth — optimal campaign windows
- **Pin-code data** reveals hyper-local growth clusters invisible at the state level
- **3 states** identified as growth hotspots — expected to cross into top tier within 2 quarters

---

## 📄 License

This project uses the [PhonePe Pulse data](https://github.com/PhonePe/pulse) which is licensed under the [Attribution-ShareAlike 4.0 International (CC BY-SA 4.0)](https://creativecommons.org/licenses/by-sa/4.0/) license.

---

*Built as part of the GUVI × HCL Data Analytics Project | Finance / Payment Systems Domain*
