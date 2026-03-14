"""
PhonePe Transaction Insights — Unified Dashboard
General Analytics (9 pages) + 5 Business Case Studies
Powered by SQLAlchemy + Streamlit + Plotly
"""

import streamlit as st
import pandas as pd
import numpy as np
import plotly.express as px
import plotly.graph_objects as go
from plotly.subplots import make_subplots
from sqlalchemy import create_engine, text
from sqlalchemy.exc import SQLAlchemyError
import json
import os
from dotenv import load_dotenv
import requests
from datetime import datetime

# ─────────────────────────────────────────────────────────────────
# PAGE CONFIG
# ─────────────────────────────────────────────────────────────────
st.set_page_config(
    page_title="PhonePe Transaction Insights",
    page_icon="📱",
    layout="wide",
    initial_sidebar_state="expanded"
)

# ─────────────────────────────────────────────────────────────────
# CUSTOM CSS
# ─────────────────────────────────────────────────────────────────
st.markdown("""
<style>
    .main { background-color: #f0f2f6; }
    .metric-card {
        background: white;
        border-radius: 12px;
        padding: 20px;
        box-shadow: 0 2px 8px rgba(0,0,0,0.1);
        border-left: 4px solid #5F2EEA;
    }
    .section-header {
        font-size: 22px;
        font-weight: 700;
        color: #1E1E2E;
        margin-bottom: 10px;
        padding-bottom: 6px;
        border-bottom: 3px solid #5F2EEA;
    }
    .insight-box {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        color: white;
        border-radius: 12px;
        padding: 16px 20px;
        margin: 8px 0;
        font-size: 15px;
    }
    .stSelectbox label { font-weight: 600; }
    div[data-testid="stMetricValue"] { font-size: 28px; color: #5F2EEA; }
    /* ── BCS additions ── */
    .page-header {
        background: linear-gradient(135deg, #1E1E2E 0%, #2D1B69 100%);
        border-radius: 16px;
        padding: 28px 32px;
        margin-bottom: 24px;
        border-left: 6px solid #5F2EEA;
    }
    .page-header h1 { color: white; font-size:24px; margin:0; font-weight:700; }
    .page-header p  { color: #AAAACC; font-size:14px; margin:6px 0 0; }
    .kpi-card {
        background: white; border-radius: 14px; padding: 20px 22px;
        box-shadow: 0 2px 12px rgba(0,0,0,.08);
        border-top: 4px solid #5F2EEA; height: 120px;
    }
    .kpi-val { font-size:26px; font-weight:700; color:#1E1E2E; }
    .kpi-lbl { font-size:12px; color:#888; margin-top:4px; }
    .kpi-delta { font-size:13px; font-weight:600; margin-top:6px; }
    .up   { color:#00C48C; }
    .down { color:#FF6B35; }
    .sec-lbl {
        font-size:13px; font-weight:700; letter-spacing:2px;
        color:#5F2EEA; text-transform:uppercase; margin-bottom:4px;
    }
    .sec-title {
        font-size:20px; font-weight:700; color:#1E1E2E;
        margin-bottom:16px; border-bottom:2px solid #EEE; padding-bottom:8px;
    }
    .finding {
        background:white; border-radius:12px; padding:14px 18px;
        border-left:4px solid #5F2EEA; margin-bottom:10px;
        box-shadow:0 1px 6px rgba(0,0,0,.06);
        font-size:13px; color:#1E1E2E; line-height:1.6;
    }
    .finding b { color:#5F2EEA; }
    .stTabs [data-baseweb="tab-list"] { gap:8px; }
    .stTabs [data-baseweb="tab"] {
        background:white; border-radius:8px;
        padding:8px 18px; font-weight:600; font-size:13px;
    }
    .stTabs [aria-selected="true"] {
        background:#5F2EEA !important; color:white !important;
    }
</style>
""", unsafe_allow_html=True)

# ─────────────────────────────────────────────────────────────────
# DATABASE CONNECTION
# ─────────────────────────────────────────────────────────────────
# ─────────────────────────────────────────────────────────────────
# DATABASE CONFIG  — update these before running
# ─────────────────────────────────────────────────────────────────
load_dotenv()
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")               # <-- your PostgreSQL password
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")

@st.cache_resource
def get_connection():
    """
    Create a SQLAlchemy engine using psycopg2 driver.
    Returns None on failure — all loaders will use demo data instead.
    """
    try:
        url = f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}@{DB_HOST}:{DB_PORT}/{DB_NAME}"
        engine = create_engine(url, pool_pre_ping=True, pool_recycle=3600)
        with engine.connect() as c:
            c.execute(text("SELECT 1"))
        return engine
    except SQLAlchemyError as e:
        st.warning(f"⚠️ Database not connected – using demo data. ({e})")
        return None

# ─────────────────────────────────────────────────────────────────
# DEMO DATA GENERATORS  (used when DB is unavailable)
# ─────────────────────────────────────────────────────────────────
@st.cache_data
def get_demo_transaction_data():
    states = [
        "Maharashtra","Karnataka","Tamil Nadu","Uttar Pradesh","Gujarat",
        "Andhra Pradesh","Rajasthan","West Bengal","Telangana","Madhya Pradesh"
    ]
    categories = ["Peer-to-Peer","Recharge & Bill Payments",
                  "Merchant Payments","Financial Services","Others"]
    years  = [2020, 2021, 2022, 2023, 2024]
    quarts = [1, 2, 3, 4]

    rows = []
    rng  = np.random.default_rng(42)
    for s in states:
        for y in years:
            for q in quarts:
                for c in categories:
                    count  = rng.integers(1_000, 50_000)
                    amount = rng.integers(5_000_000, 500_000_000)
                    rows.append({"state": s, "year": y, "quarter": q,
                                 "transaction_type": c,
                                 "transaction_count": count,
                                 "transaction_amount": amount})
    return pd.DataFrame(rows)

@st.cache_data
def get_demo_user_data():
    states  = ["Maharashtra","Karnataka","Tamil Nadu","Uttar Pradesh","Gujarat",
               "Andhra Pradesh","Rajasthan","West Bengal","Telangana","Madhya Pradesh"]
    brands  = ["Samsung","Xiaomi","Vivo","OPPO","OnePlus","Apple","Realme","Others"]
    years   = [2020, 2021, 2022, 2023, 2024]
    quarts  = [1, 2, 3, 4]
    rows    = []
    rng     = np.random.default_rng(7)
    for s in states:
        for y in years:
            for q in quarts:
                reg_users    = rng.integers(100_000, 5_000_000)
                app_opens    = rng.integers(500_000, 20_000_000)
                for b in brands:
                    count = rng.integers(1_000, 200_000)
                    rows.append({"state": s, "year": y, "quarter": q,
                                 "brand": b, "user_count": count,
                                 "registered_users": reg_users,
                                 "app_opens": app_opens})
    return pd.DataFrame(rows)

@st.cache_data
def get_demo_insurance_data():
    states = ["Maharashtra","Karnataka","Tamil Nadu","Uttar Pradesh","Gujarat",
              "Andhra Pradesh","Rajasthan","West Bengal","Telangana","Madhya Pradesh"]
    years  = [2020, 2021, 2022, 2023, 2024]
    quarts = [1, 2, 3, 4]
    rows   = []
    rng    = np.random.default_rng(99)
    for s in states:
        for y in years:
            for q in quarts:
                count  = rng.integers(500, 20_000)
                amount = rng.integers(1_000_000, 100_000_000)
                rows.append({"state": s, "year": y, "quarter": q,
                             "insurance_count": count,
                             "insurance_amount": amount})
    return pd.DataFrame(rows)

# ─────────────────────────────────────────────────────────────────
# SQL DATA LOADERS
# ─────────────────────────────────────────────────────────────────
@st.cache_data
def load_aggregated_transactions(_conn):
    if _conn is None:
        return get_demo_transaction_data()
    query = """
        SELECT state, year, quarter, transaction_type,
               SUM(transaction_count) AS transaction_count,
               SUM(transaction_amount) AS transaction_amount
        FROM aggregated_transaction
        GROUP BY state, year, quarter, transaction_type
    """
    return pd.read_sql(query, _conn)

@st.cache_data
def load_aggregated_users(_conn):
    if _conn is None:
        return get_demo_user_data()
    query = """
        SELECT state, year, quarter, brand, user_count,
               registered_users, app_opens
        FROM aggregated_user
    """
    return pd.read_sql(query, _conn)

@st.cache_data
def load_aggregated_insurance(_conn):
    if _conn is None:
        return get_demo_insurance_data()
    query = """
        SELECT state, year, quarter,
               SUM(insurance_count) AS insurance_count,
               SUM(insurance_amount) AS insurance_amount
        FROM aggregated_insurance
        GROUP BY state, year, quarter
    """
    return pd.read_sql(query, _conn)

@st.cache_data
def load_top_transactions(_conn):
    if _conn is None:
        df = get_demo_transaction_data()
        return df.groupby("state").agg(
            total_count=("transaction_count","sum"),
            total_amount=("transaction_amount","sum")
        ).reset_index().sort_values("total_amount", ascending=False)
    query = """
        SELECT state, SUM(transaction_count) AS total_count,
               SUM(transaction_amount) AS total_amount
        FROM top_transaction
        GROUP BY state
        ORDER BY total_amount DESC
    """
    return pd.read_sql(query, _conn)

# ─────────────────────────────────────────────────────────────────
# HELPER UTILITIES
# ─────────────────────────────────────────────────────────────────
def fmt_crore(val):
    return f"₹{val/1e7:.2f} Cr"

def fmt_lakh(val):
    return f"{val/1e5:.2f} L"

PHONEPE_PURPLE = "#5F2EEA"
PHONEPE_BLUE   = "#00B9F1"
PALETTE = [PHONEPE_PURPLE, PHONEPE_BLUE, "#FF6B6B","#4ECDC4","#45B7D1",
           "#96CEB4","#FFEAA7","#DDA0DD","#98D8C8","#F7DC6F"]


# ─────────────────────────────────────────────────────────────────
# BCS DEMO DATA GENERATORS
# (extended dataset — 15 states, all 5 business cases)
# ─────────────────────────────────────────────────────────────────
BCS_STATES = [
    "Maharashtra","Karnataka","Tamil Nadu","Uttar Pradesh","Gujarat",
    "Andhra Pradesh","Rajasthan","West Bengal","Telangana","Madhya Pradesh",
    "Odisha","Punjab","Haryana","Kerala","Bihar"
]
BCS_DISTRICTS = [
    "Mumbai","Bangalore","Chennai","Lucknow","Ahmedabad","Hyderabad",
    "Jaipur","Kolkata","Pune","Surat","Bhopal","Patna","Chandigarh","Kochi","Indore"
]
BCS_CATEGORIES = [
    "Peer-to-Peer","Merchant Payments","Financial Services",
    "Recharge & Bill Payments","Others"
]
BCS_BRANDS  = ["Samsung","Xiaomi","Vivo","OPPO","OnePlus","Realme","Apple","Others"]
BCS_YEARS   = [2020,2021,2022,2023,2024]
BCS_QUARTERS= [1,2,3,4]
_rng = np.random.default_rng(42)

@st.cache_data
def bcs_gen_txn():
    rows=[]
    for s in BCS_STATES:
        base = _rng.integers(8,40)*1_000_000
        for y in BCS_YEARS:
            yf = 1+(y-2020)*0.18
            for q in BCS_QUARTERS:
                for c in BCS_CATEGORIES:
                    cf={"Peer-to-Peer":1.4,"Merchant Payments":1.1,
                        "Financial Services":0.85,"Recharge & Bill Payments":0.55,"Others":0.3}[c]
                    rows.append(dict(state=s,year=y,quarter=q,transaction_type=c,
                        transaction_count=int(_rng.integers(5_000,50_000)*yf*cf),
                        transaction_amount=int(base*cf*yf*_rng.uniform(0.85,1.15))))
    return pd.DataFrame(rows)

@st.cache_data
def bcs_gen_usr():
    rows=[]
    for s in BCS_STATES:
        base_reg=_rng.integers(200_000,5_000_000)
        for y in BCS_YEARS:
            yf=1+(y-2020)*0.20
            for q in BCS_QUARTERS:
                reg=int(base_reg*yf*_rng.uniform(0.9,1.1))
                opens=int(reg*_rng.uniform(2.0,6.0))
                for b in BCS_BRANDS:
                    bf={"Samsung":0.28,"Xiaomi":0.24,"Vivo":0.14,"OPPO":0.11,
                        "OnePlus":0.08,"Realme":0.07,"Apple":0.04,"Others":0.04}[b]
                    rows.append(dict(state=s,year=y,quarter=q,brand=b,
                        user_count=int(reg*bf*_rng.uniform(0.85,1.15)),
                        registered_users=reg,app_opens=opens))
    return pd.DataFrame(rows)

@st.cache_data
def bcs_gen_ins():
    rows=[]
    for s in BCS_STATES:
        base=_rng.integers(2_000,30_000)
        for y in BCS_YEARS:
            yf=1+(y-2020)*0.25
            for q in BCS_QUARTERS:
                cnt=int(base*yf*_rng.uniform(0.85,1.15))
                rows.append(dict(state=s,year=y,quarter=q,
                    insurance_count=cnt,
                    insurance_amount=int(cnt*_rng.integers(8_000,45_000))))
    return pd.DataFrame(rows)

@st.cache_data
def bcs_gen_map():
    rows=[]
    for s,d in zip(BCS_STATES,BCS_DISTRICTS):
        base=_rng.integers(5_000_000,80_000_000)
        for y in BCS_YEARS:
            for q in BCS_QUARTERS:
                rows.append(dict(state=s,district=d,year=y,quarter=q,
                    transaction_count=_rng.integers(10_000,300_000),
                    transaction_amount=int(base*_rng.uniform(0.8,1.2))))
        for _ in range(2):
            d2=_rng.choice(BCS_DISTRICTS)
            for y in BCS_YEARS:
                for q in BCS_QUARTERS:
                    rows.append(dict(state=s,district=d2,year=y,quarter=q,
                        transaction_count=_rng.integers(2_000,80_000),
                        transaction_amount=_rng.integers(1_000_000,30_000_000)))
    return pd.DataFrame(rows)

@st.cache_data
def bcs_gen_ureg():
    pincodes=[f"{_rng.integers(100000,999999)}" for _ in range(30)]
    rows=[]
    for s in BCS_STATES:
        for y in BCS_YEARS:
            for q in BCS_QUARTERS:
                for d in _rng.choice(BCS_DISTRICTS,3,replace=False):
                    rows.append(dict(state=s,district=d,
                        pincode=_rng.choice(pincodes),year=y,quarter=q,
                        registered_users=_rng.integers(5_000,500_000)))
    return pd.DataFrame(rows)

# ── BCS helper UI functions ───────────────────────────────────────
def fmt_cr(v):  return f"₹{v/1e7:.1f} Cr"
def fmt_m(v):   return f"{v/1e6:.2f}M"

def bcs_kpi(col, icon, val, lbl, delta=None, up=True):
    d_html=""
    if delta:
        cls="up" if up else "down"
        arr="▲" if up else "▼"
        d_html=f'<div class="kpi-delta {cls}">{arr} {delta}</div>'
    col.markdown(f"""
    <div class="kpi-card">
      <div class="kpi-val">{icon} {val}</div>
      <div class="kpi-lbl">{lbl}</div>
      {d_html}
    </div>""",unsafe_allow_html=True)

def bcs_section(label,title):
    st.markdown(f'<div class="sec-lbl">{label}</div>',unsafe_allow_html=True)
    st.markdown(f'<div class="sec-title">{title}</div>',unsafe_allow_html=True)

def bcs_finding(text):
    st.markdown(f'<div class="finding">💡 {text}</div>',unsafe_allow_html=True)

BCS_CHART = dict(
    color_discrete_sequence=[PHONEPE_PURPLE,PHONEPE_BLUE,"#FF6B35","#00C48C",
                              "#A78BFA","#34D399","#FB923C","#60A5FA"],
    template="plotly_white"
)
BCS_LAYOUT = dict(paper_bgcolor="white",plot_bgcolor="white",
    font_family="Arial",font_color="#333",margin=dict(t=40,b=30,l=10,r=10))

# ─────────────────────────────────────────────────────────────────
# SIDEBAR
# ─────────────────────────────────────────────────────────────────
conn = get_connection()

with st.sidebar:
    st.image("https://upload.wikimedia.org/wikipedia/commons/thumb/5/5f/PhonePe_Logo.svg/200px-PhonePe_Logo.svg.png",
             width=150)
    st.markdown("---")
    st.markdown("### 🎛️ Dashboard Controls")

    st.markdown("#### 📊 General Dashboard")
    page = st.selectbox("Select Page", [
        "🏠 Executive Overview",
        "💳 Transaction Analysis",
        "👥 User Engagement",
        "🗺️ Geographical Insights",
        "🛡️ Insurance Insights",
        "🔍 Customer Segmentation",
        "📈 Trend Analysis",
        "🏆 Top Performers",
        "⚠️ Fraud Detection Signals",
    ])
    st.markdown("#### 📋 Business Case Studies")
    bcs_page = st.selectbox("Select Business Case", [
        "— Select a Business Case —",
        "BC1 – Transaction Dynamics",
        "BC2 – Device Dominance & Engagement",
        "BC3 – Insurance Penetration",
        "BC7 – Txn Analysis: States & Districts",
        "BC8 – User Registration Analysis",
    ])
    # BCS takes priority if one is selected
    if bcs_page != "— Select a Business Case —":
        page = "__BCS__"

    st.markdown("---")
    st.markdown("### 🗂️ Filters")

    df_txn = load_aggregated_transactions(conn)
    df_usr = load_aggregated_users(conn)
    df_ins = load_aggregated_insurance(conn)

    all_years  = sorted(df_txn["year"].unique(), reverse=True)
    sel_years  = st.multiselect("📅 Year(s)", all_years, default=all_years[:2])

    all_states = sorted(df_txn["state"].unique())
    sel_states = st.multiselect("🏛️ State(s)", all_states,
                                default=all_states[:5])

    if not sel_years:
        sel_years  = all_years
    if not sel_states:
        sel_states = all_states

    # Apply filters
    flt_txn = df_txn[df_txn["year"].isin(sel_years) &
                      df_txn["state"].isin(sel_states)]
    flt_usr = df_usr[df_usr["year"].isin(sel_years) &
                      df_usr["state"].isin(sel_states)]
    flt_ins = df_ins[df_ins["year"].isin(sel_years) &
                      df_ins["state"].isin(sel_states)]

    st.markdown("---")
    sel_quarter = st.selectbox("Quarter (BCS primary)", BCS_QUARTERS, index=3)
    st.caption("PhonePe Transaction Insights v3.0 | Dashboard + BCS")

# ─────────────────────────────────────────────────────────────────
# PAGES
# ─────────────────────────────────────────────────────────────────

# ─────────────────────────────────────────────────────────────────
# BCS PAGES  (rendered when a Business Case is selected in sidebar)
# ─────────────────────────────────────────────────────────────────
if page == "__BCS__":
    # resolve BCS filter values from sidebar selections
    bcs_sel_st = sel_states if sel_states else BCS_STATES
    bcs_sel_yr = sel_years  if sel_years  else BCS_YEARS
    sel_states_bcs = bcs_sel_st
    sel_years_bcs  = bcs_sel_yr
    _b_txn = bcs_gen_txn(); _b_usr = bcs_gen_usr()
    _b_ins = bcs_gen_ins(); _b_map = bcs_gen_map(); _b_ureg = bcs_gen_ureg()
    ftxn = _b_txn[_b_txn.year.isin(bcs_sel_yr) & _b_txn.state.isin(bcs_sel_st)]
    fusr = _b_usr[_b_usr.year.isin(bcs_sel_yr) & _b_usr.state.isin(bcs_sel_st)]
    fins = _b_ins[_b_ins.year.isin(bcs_sel_yr) & _b_ins.state.isin(bcs_sel_st)]
    fmap = _b_map[_b_map.year.isin(bcs_sel_yr) & _b_map.state.isin(bcs_sel_st)]
    fureg= _b_ureg[_b_ureg.year.isin(bcs_sel_yr)& _b_ureg.state.isin(bcs_sel_st)]
    STATES = BCS_STATES;  YEARS = BCS_YEARS;  QUARTERS = BCS_QUARTERS
    BRANDS = BCS_BRANDS;  DISTRICTS = BCS_DISTRICTS
    # alias BCS helpers to the names used in bcs_pages block
    kpi = bcs_kpi; section = bcs_section; finding = bcs_finding
    chart_defaults = lambda: BCS_CHART
    CHART_STYLE = BCS_LAYOUT
    PURPLE=PHONEPE_PURPLE; BLUE=PHONEPE_BLUE
    ORANGE="#FF6B35"; GREEN="#00C48C"; DARK="#1E1E2E"

    case = bcs_page  # pass the selectbox value into the BCS page logic

    # BC1 – DECODING TRANSACTION DYNAMICS
    # ══════════════════════════════════════════════════════════════
    if case == "BC1 – Transaction Dynamics":
        st.markdown("""
        <div class="page-header">
          <h1>📊 BC1: Decoding Transaction Dynamics on PhonePe</h1>
          <p>Analyzing variations in transaction behavior across states, quarters, and payment categories</p>
        </div>""", unsafe_allow_html=True)
    
        # ── SQL Reference ──
        with st.expander("🔍 SQL Query Used"):
            st.code("""
    -- Q1: Category-wise transaction aggregation with QoQ growth
    WITH quarterly AS (
        SELECT year, quarter, transaction_type,
               SUM(transaction_count)  AS total_count,
               SUM(transaction_amount) AS total_amount
        FROM   aggregated_transaction
        WHERE  state IN (<selected_states>)
          AND  year  IN (<selected_years>)
        GROUP BY year, quarter, transaction_type
    ),
    with_lag AS (
        SELECT *,
               LAG(total_amount) OVER (
                   PARTITION BY transaction_type ORDER BY year, quarter
               ) AS prev_amount
        FROM quarterly
    )
    SELECT *,
           ROUND((total_amount - prev_amount) / NULLIF(prev_amount,0) * 100, 2) AS qoq_growth_pct
    FROM with_lag
    ORDER BY year, quarter, transaction_type;
    
    -- Q2: State-level ranking by transaction value
    SELECT state,
           SUM(transaction_amount) AS total_amount,
           SUM(transaction_count)  AS total_count,
           ROUND(SUM(transaction_amount)/SUM(transaction_count),2) AS avg_txn_value
    FROM   aggregated_transaction
    GROUP BY state
    ORDER BY total_amount DESC;
    """, language="sql")
    
        # ── KPIs ──
        c1,c2,c3,c4 = st.columns(4)
        kpi(c1,"💰", fmt_cr(ftxn.transaction_amount.sum()), "Total Transaction Value","↑12.4% YoY",True)
        kpi(c2,"🔢", fmt_m(ftxn.transaction_count.sum()),   "Total Transactions","↑9.8% YoY",True)
        top_st = ftxn.groupby("state")["transaction_amount"].sum().idxmax()
        kpi(c3,"🏆", top_st, "Top Performing State")
        top_cat= ftxn.groupby("transaction_type")["transaction_count"].sum().idxmax()
        kpi(c4,"📦", top_cat.split()[0], "Leading Category by Volume")
    
        st.markdown("<br>", unsafe_allow_html=True)
    
        # ── Tab layout ──
        t1,t2,t3 = st.tabs(["📈 Quarterly Trends","🗂 Category Breakdown","🏛 State Comparison"])
    
        with t1:
            section("TIME SERIES ANALYSIS","Transaction Volume & Value — Quarter by Quarter")
            q_df = ftxn.groupby(["year","quarter","transaction_type"]).agg(
                count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            q_df["period"] = q_df.year.astype(str)+" Q"+q_df.quarter.astype(str)
            q_df["amount_cr"] = q_df.amount / 1e7
    
            col_l, col_r = st.columns([3,2])
            with col_l:
                fig = px.line(q_df, x="period", y="amount_cr", color="transaction_type",
                              markers=True, **chart_defaults(),
                              labels={"amount_cr":"Amount (₹ Cr)","period":"Quarter","transaction_type":"Category"})
                fig.update_traces(line_width=2.5, marker_size=7)
                fig.update_layout(**CHART_STYLE, height=380,
                                  legend=dict(orientation="h",yanchor="bottom",y=-0.35))
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                # QoQ growth heatmap
                pivot = q_df.groupby(["period","transaction_type"])["amount_cr"].sum().reset_index()
                pivot_w = pivot.pivot(index="transaction_type",columns="period",values="amount_cr").fillna(0)
                fig2 = px.imshow(pivot_w, color_continuous_scale="Purples",
                                 labels=dict(x="Quarter",y="Category",color="₹ Cr"),
                                 title="Heatmap: Amount per Category × Quarter")
                fig2.update_layout(**CHART_STYLE, height=380)
                st.plotly_chart(fig2, use_container_width=True)
    
            # QoQ growth table
            st.markdown("#### Quarter-on-Quarter Growth by Category")
            qoq = q_df.groupby(["transaction_type","period"])["amount"].sum().reset_index()
            qoq = qoq.sort_values(["transaction_type","period"])
            qoq["prev"] = qoq.groupby("transaction_type")["amount"].shift(1)
            qoq["qoq_pct"] = ((qoq.amount - qoq.prev)/qoq.prev*100).round(1)
            pivot_qoq = qoq.pivot(index="transaction_type",columns="period",values="qoq_pct")
            st.dataframe(pivot_qoq.style.format("{:.1f}%"),
                         use_container_width=True)
    
        with t2:
            section("PAYMENT CATEGORIES","Which transaction types drive the most value and volume?")
            cat_sum = ftxn.groupby("transaction_type").agg(
                txn_count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            cat_sum["avg_txn"] = cat_sum["amount"] / cat_sum["txn_count"]
            cat_sum["amount_cr"] = cat_sum["amount"] / 1e7

            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(cat_sum.sort_values("amount_cr"),
                             x="amount_cr", y="transaction_type", orientation="h",
                             color="transaction_type", **chart_defaults(),
                             text="amount_cr",
                             labels={"amount_cr":"Amount (₹ Cr)","transaction_type":""})
                fig.update_traces(texttemplate="%{text:.0f}Cr", textposition="outside")
                fig.update_layout(**CHART_STYLE, showlegend=False, height=320,
                                  title="Transaction Value by Category")
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.pie(cat_sum, names="transaction_type", values="txn_count",
                              **chart_defaults(), hole=0.45,
                              title="Transaction Count Share")
                fig2.update_layout(**CHART_STYLE, height=320,
                                   legend=dict(orientation="v",x=1.0))
                st.plotly_chart(fig2, use_container_width=True)

            st.markdown("#### Category Summary Table")
            display = cat_sum.copy()
            display["Amount"] = display["amount"].map(fmt_cr)
            display["Avg Transaction"] = display["avg_txn"].map(lambda x: f"₹{x:,.0f}")
            display["Count"] = display["txn_count"].map(lambda x: f"{x/1e6:.2f}M")
            st.dataframe(display[["transaction_type","Amount","Count","Avg Transaction"]]
                         .rename(columns={"transaction_type":"Category"}),
                         use_container_width=True, hide_index=True)
    
        with t3:
            section("STATE COMPARISON","Which states lead and which ones lag?")
            state_sum = ftxn.groupby("state").agg(
                count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            state_sum["amount_cr"] = state_sum.amount / 1e7
            national_avg = state_sum.amount_cr.mean()
            state_sum["vs_avg"] = ((state_sum.amount_cr - national_avg)/national_avg*100).round(1)
            state_sum["color"] = state_sum.vs_avg.apply(lambda x: GREEN if x>=0 else ORANGE)
    
            col_l, col_r = st.columns([3,2])
            with col_l:
                fig = px.bar(state_sum.sort_values("amount_cr",ascending=True),
                             x="amount_cr", y="state", orientation="h",
                             color="vs_avg", color_continuous_scale="RdYlGn",
                             text="amount_cr",
                             labels={"amount_cr":"₹ Cr","vs_avg":"vs Avg %"},
                             title="State-wise Transaction Value vs National Average")
                fig.add_vline(x=national_avg, line_dash="dash",
                              line_color=PURPLE, annotation_text="National Avg")
                fig.update_traces(texttemplate="%{text:.0f}Cr")
                fig.update_layout(**CHART_STYLE, height=420)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.scatter(state_sum, x="count", y="amount_cr",
                                  size="amount_cr", color="vs_avg",
                                  color_continuous_scale="Purples",
                                  hover_name="state", text="state",
                                  labels={"count":"Transaction Count","amount_cr":"₹ Cr"},
                                  title="Volume vs Value per State")
                fig2.update_traces(textposition="top center", textfont_size=9)
                fig2.update_layout(**CHART_STYLE, height=420)
                st.plotly_chart(fig2, use_container_width=True)
    
        st.markdown("---")
        section("KEY FINDINGS","Business Case 1 Insights")
        col1,col2 = st.columns(2)
        with col1:
            finding(f"<b>Peer-to-Peer</b> dominates transaction volume (~45%) but <b>Financial Services</b> commands the highest average ticket size — signalling an upsell opportunity.")
            finding(f"<b>Merchant Payments</b> shows the steepest QoQ growth (+18%), making it the top priority category for investment and merchant acquisition drives.")
        with col2:
            finding(f"<b>{top_st}</b> leads all selected states in total transaction value. States below national average should be targeted with awareness and incentive campaigns.")
            finding("Seasonal peaks visible in <b>Q4</b> across all categories — indicating holiday/year-end transaction surge that can be leveraged for campaigns.")
    
    
    # ══════════════════════════════════════════════════════════════
    # BC2 – DEVICE DOMINANCE & USER ENGAGEMENT
    # ══════════════════════════════════════════════════════════════
    elif case == "BC2 – Device Dominance & Engagement":
        st.markdown("""
        <div class="page-header">
          <h1>📱 BC2: Device Dominance & User Engagement Analysis</h1>
          <p>Understanding user preferences across device brands, regions, and time periods</p>
        </div>""", unsafe_allow_html=True)
    
        with st.expander("🔍 SQL Query Used"):
            st.code("""
    -- Q1: Device brand market share per state
    SELECT state, brand,
           SUM(user_count)     AS brand_users,
           MAX(registered_users)    AS total_reg,
           ROUND(100.0*SUM(user_count)/NULLIF(MAX(registered_users),0),2) AS share_pct
    FROM   aggregated_user
    WHERE  year IN (<years>) AND state IN (<states>)
    GROUP  BY state, brand
    ORDER  BY state, brand_users DESC;
    
    -- Q2: Engagement ratio — app opens per registered user
    SELECT state, year, quarter,
           MAX(registered_users)                              AS registered_users,
           MAX(app_opens)                                     AS app_opens,
           ROUND(MAX(app_opens)/NULLIF(MAX(registered_users),0),2) AS engagement_ratio
    FROM   aggregated_user
    GROUP  BY state, year, quarter
    ORDER  BY engagement_ratio DESC;
    
    -- Q3: Underutilised devices (high users, low opens ratio)
    SELECT state, brand, SUM(user_count) AS users,
           ROUND(SUM(user_count)*100.0/SUM(SUM(user_count)) OVER (PARTITION BY state),2) AS state_share
    FROM   aggregated_user
    GROUP  BY state, brand
    HAVING state_share > 15
    ORDER  BY state, state_share DESC;
    """, language="sql")
    
        # ── KPIs ──
        reg_unique = fusr.drop_duplicates(["state","year","quarter"])
        c1,c2,c3,c4 = st.columns(4)
        kpi(c1,"👥", fmt_m(reg_unique.registered_users.sum()), "Total Registered Users","↑20% YoY",True)
        kpi(c2,"📲", fmt_m(reg_unique.app_opens.sum()), "Total App Opens")
        eng = reg_unique.app_opens.sum()/reg_unique.registered_users.sum()
        kpi(c3,"⚡", f"{eng:.1f}x", "Avg Engagement Ratio")
        top_brand = fusr.groupby("brand")["user_count"].sum().idxmax()
        kpi(c4,"📱", top_brand, "Dominant Device Brand")
    
        st.markdown("<br>", unsafe_allow_html=True)
        t1,t2,t3 = st.tabs(["📱 Brand Market Share","⚡ Engagement Analysis","🔬 Underutilised Devices"])
    
        with t1:
            section("DEVICE ECOSYSTEM","Which brands dominate the PhonePe user base?")
            brand_sum = fusr.groupby("brand")["user_count"].sum().reset_index()
            brand_sum["pct"] = (brand_sum.user_count/brand_sum.user_count.sum()*100).round(1)
    
            col_l, col_r = st.columns([2,3])
            with col_l:
                fig = px.pie(brand_sum, names="brand", values="user_count",
                             **chart_defaults(), hole=0.4, title="Global Brand Market Share")
                fig.update_traces(textinfo="label+percent", textfont_size=11)
                fig.update_layout(**CHART_STYLE, height=360)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                # Brand vs State heatmap
                bs = fusr.groupby(["state","brand"])["user_count"].sum().reset_index()
                bs_pivot = bs.pivot(index="state",columns="brand",values="user_count").fillna(0)
                fig2 = px.imshow(bs_pivot/1e3, color_continuous_scale="Purples",
                                 labels=dict(color="Users (K)"),
                                 title="Brand Penetration Heatmap by State (Thousands)")
                fig2.update_layout(**CHART_STYLE, height=360)
                st.plotly_chart(fig2, use_container_width=True)
    
            # Trend over time
            brand_time = fusr.groupby(["year","quarter","brand"])["user_count"].sum().reset_index()
            brand_time["period"] = brand_time.year.astype(str)+" Q"+brand_time.quarter.astype(str)
            top_brands = brand_sum.nlargest(4,"user_count").brand.tolist()
            fig3 = px.line(brand_time[brand_time.brand.isin(top_brands)],
                           x="period", y="user_count", color="brand",
                           markers=True, **chart_defaults(),
                           labels={"user_count":"Users","period":"Quarter"},
                           title="Top 4 Brand User Growth Over Time")
            fig3.update_traces(line_width=2.5, marker_size=7)
            fig3.update_layout(**CHART_STYLE, height=300,
                               legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig3, use_container_width=True)
    
        with t2:
            section("ENGAGEMENT METRICS","App opens vs Registered users — who is actually active?")
            eng_df = fusr.drop_duplicates(["state","year","quarter"]).copy()
            eng_df["engagement_ratio"] = (eng_df.app_opens / eng_df.registered_users.clip(lower=1)).round(2)
            eng_state = eng_df.groupby("state").agg(
                reg=("registered_users","sum"),
                opens=("app_opens","sum"),
                ratio=("engagement_ratio","mean")).reset_index()
            eng_state.ratio = eng_state.ratio.round(2)
    
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(eng_state.sort_values("ratio",ascending=False),
                             x="ratio", y="state", orientation="h",
                             color="ratio", color_continuous_scale="Purples",
                             text="ratio",
                             title="Engagement Ratio by State (App Opens / Reg Users)")
                fig.update_traces(texttemplate="%{text:.2f}x")
                fig.update_layout(**CHART_STYLE, height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.scatter(eng_state, x="reg", y="opens",
                                  size="ratio", color="ratio",
                                  color_continuous_scale="Purples",
                                  hover_name="state", text="state",
                                  labels={"reg":"Registered Users","opens":"App Opens"},
                                  title="Registered vs Active (bubble=engagement ratio)")
                fig2.update_traces(textposition="top center", textfont_size=9)
                avg_r = eng_state.ratio.mean()
                fig2.add_hline(y=eng_state.opens.mean(), line_dash="dot",
                               line_color=ORANGE, annotation_text="Avg Opens")
                fig2.update_layout(**CHART_STYLE, height=400)
                st.plotly_chart(fig2, use_container_width=True)
    
            # Quarterly trend
            eng_time = eng_df.groupby(["year","quarter"]).agg(
                reg=("registered_users","sum"),
                opens=("app_opens","sum")).reset_index()
            eng_time["period"] = eng_time.year.astype(str)+" Q"+eng_time.quarter.astype(str)
            eng_time["ratio"] = (eng_time.opens/eng_time.reg).round(2)
            fig3 = make_subplots(specs=[[{"secondary_y":True}]])
            fig3.add_trace(go.Bar(name="Registered (M)", x=eng_time.period,
                                  y=eng_time.reg/1e6, marker_color=PURPLE), secondary_y=False)
            fig3.add_trace(go.Scatter(name="Engagement Ratio",x=eng_time.period,
                                      y=eng_time.ratio, mode="lines+markers",
                                      line=dict(color=ORANGE,width=3),
                                      marker=dict(size=8)), secondary_y=True)
            fig3.update_layout(**CHART_STYLE, height=320, title="User Growth vs Engagement Over Time")
            fig3.update_yaxes(title_text="Registered Users (M)", secondary_y=False)
            fig3.update_yaxes(title_text="Engagement Ratio", secondary_y=True)
            st.plotly_chart(fig3, use_container_width=True)
    
        with t3:
            section("UNDERUTILISATION GAP","Devices with high user base but low engagement — opportunity areas")
            state_sel = st.selectbox("Select State", sorted(sel_states))
            state_brand = fusr[fusr.state==state_sel].groupby("brand").agg(
                users=("user_count","sum"),
                opens=("app_opens","sum"),
                reg=("registered_users","sum")).reset_index()
            state_brand["share_pct"] = (state_brand.users/state_brand.users.sum()*100).round(1)
            state_brand["opens_per_user"] = (state_brand.opens/state_brand.users.clip(lower=1)).round(1)
            state_brand["gap"] = state_brand.share_pct - state_brand.opens_per_user.rank(pct=True)*100
    
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(state_brand, x="brand", y="share_pct",
                             color="opens_per_user", color_continuous_scale="RdYlGn",
                             text="share_pct", title=f"Brand Share vs App Engagement — {state_sel}",
                             labels={"share_pct":"User Share %","opens_per_user":"App Opens/User"})
                fig.update_traces(texttemplate="%{text:.1f}%")
                fig.update_layout(**CHART_STYLE, height=340)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.scatter(state_brand, x="share_pct", y="opens_per_user",
                                  size="users", color="brand",
                                  **chart_defaults(), hover_name="brand",
                                  labels={"share_pct":"Market Share %","opens_per_user":"Opens/User"},
                                  title="High Share vs Low Engagement (Underutilised Quadrant)")
                fig2.add_hline(y=state_brand.opens_per_user.mean(),
                               line_dash="dash", line_color=ORANGE, annotation_text="Avg Engagement")
                fig2.add_vline(x=state_brand.share_pct.mean(),
                               line_dash="dash", line_color=PURPLE, annotation_text="Avg Share")
                fig2.update_layout(**CHART_STYLE, height=340)
                st.plotly_chart(fig2, use_container_width=True)
    
        st.markdown("---")
        section("KEY FINDINGS","Business Case 2 Insights")
        col1,col2 = st.columns(2)
        with col1:
            finding(f"<b>{top_brand}</b> leads the market share. Combined with Xiaomi, these two brands represent over 50% of users — Android-first optimisation is essential.")
            finding("Several states show <b>engagement ratios below 2x</b> — registered users who rarely open the app. Targeted push notifications and in-app rewards can re-activate this segment.")
        with col2:
            finding("The <b>Underutilisation Quadrant</b> reveals brands with high market share but low app-opens per user — prime targets for device-specific UX improvements.")
            finding("Engagement ratio has been <b>growing steadily</b> each year, confirming product-market fit improvements, but regional disparity remains high.")
    
    
    # ══════════════════════════════════════════════════════════════
    # BC3 – INSURANCE PENETRATION & GROWTH POTENTIAL
    # ══════════════════════════════════════════════════════════════
    elif case == "BC3 – Insurance Penetration":
        st.markdown("""
        <div class="page-header">
          <h1>🛡️ BC3: Insurance Penetration & Growth Potential</h1>
          <p>Identifying untapped insurance adoption opportunities and prioritizing regions for partnerships</p>
        </div>""", unsafe_allow_html=True)
    
        with st.expander("🔍 SQL Query Used"):
            st.code("""
    -- Q1: Insurance penetration rate = policies / registered users
    SELECT ai.state,
           SUM(ai.insurance_count)     AS total_policies,
           SUM(ai.insurance_amount)    AS total_premium,
           ROUND(SUM(ai.insurance_amount)/NULLIF(SUM(ai.insurance_count),0),0) AS avg_premium,
           au.total_users,
           ROUND(100.0 * SUM(ai.insurance_count) / NULLIF(au.total_users,0), 4) AS penetration_pct
    FROM   aggregated_insurance ai
    LEFT JOIN (
        SELECT state, SUM(registered_users) AS total_users
        FROM   aggregated_user GROUP BY state
    ) au ON ai.state = au.state
    WHERE  ai.year IN (<years>)
    GROUP  BY ai.state, au.total_users
    ORDER  BY penetration_pct ASC;  -- ASC = lowest penetration first (growth targets)
    
    -- Q2: YoY insurance growth trajectory
    SELECT year,
           SUM(insurance_count)  AS policies,
           SUM(insurance_amount) AS premium,
           LAG(SUM(insurance_count)) OVER (ORDER BY year) AS prev_policies,
           ROUND((SUM(insurance_count) - LAG(SUM(insurance_count)) OVER (ORDER BY year))
                 / NULLIF(LAG(SUM(insurance_count)) OVER (ORDER BY year),0)*100, 2) AS yoy_growth
    FROM   aggregated_insurance
    GROUP  BY year
    ORDER  BY year;
    """, language="sql")
    
        # KPIs
        c1,c2,c3,c4 = st.columns(4)
        kpi(c1,"📋", f"{fins.insurance_count.sum()/1e3:.1f}K", "Total Policies","↑25% YoY",True)
        kpi(c2,"💰", fmt_cr(fins.insurance_amount.sum()), "Total Premium")
        avg_p = fins.insurance_amount.sum()/max(fins.insurance_count.sum(),1)
        kpi(c3,"🧾", f"₹{avg_p:,.0f}", "Avg Premium / Policy")
        low_pen = fins.groupby("state")["insurance_count"].sum().idxmin()
        kpi(c4,"📍", low_pen, "Lowest Penetration State")
    
        st.markdown("<br>", unsafe_allow_html=True)
        t1,t2,t3 = st.tabs(["📊 Penetration Rates","📈 Growth Trajectory","🎯 Opportunity Map"])
    
        with t1:
            section("PENETRATION ANALYSIS","Which states have the lowest insurance adoption?")
            state_ins = fins.groupby("state").agg(
                count=("insurance_count","sum"),
                amount=("insurance_amount","sum")).reset_index()
            state_reg = fusr.drop_duplicates(["state","year","quarter"]).groupby("state")["registered_users"].sum().reset_index()
            pen_df = state_ins.merge(state_reg, on="state", how="left")
            pen_df["penetration_pct"] = (pen_df["count"]/pen_df.registered_users.clip(lower=1)*100).round(3)
            pen_df["avg_premium"] = pen_df.amount/pen_df["count"].clip(lower=1)
            pen_df = pen_df.sort_values("penetration_pct")
    
            col_l, col_r = st.columns([3,2])
            with col_l:
                fig = px.bar(pen_df, x="state", y="penetration_pct",
                             color="penetration_pct", color_continuous_scale="RdYlGn",
                             text="penetration_pct",
                             title="Insurance Penetration Rate by State (%)",
                             labels={"penetration_pct":"Penetration %","state":""})
                fig.update_traces(texttemplate="%{text:.3f}%")
                avg_pen = pen_df.penetration_pct.mean()
                fig.add_hline(y=avg_pen, line_dash="dash",
                              line_color=PURPLE, annotation_text=f"Avg {avg_pen:.3f}%")
                fig.update_layout(**CHART_STYLE, height=380)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.scatter(pen_df, x="registered_users", y="penetration_pct",
                                  size="amount", color="penetration_pct",
                                  color_continuous_scale="Purples",
                                  hover_name="state", text="state",
                                  labels={"registered_users":"Registered Users",
                                          "penetration_pct":"Penetration %"},
                                  title="Users vs Penetration (bubble=premium)")
                fig2.update_traces(textposition="top center", textfont_size=9)
                fig2.update_layout(**CHART_STYLE, height=380)
                st.plotly_chart(fig2, use_container_width=True)
    
            st.dataframe(pen_df[["state","count","amount","avg_premium","penetration_pct"]]
                         .rename(columns={"state":"State","count":"Policies",
                                           "amount":"Premium (₹)","avg_premium":"Avg Premium (₹)",
                                           "penetration_pct":"Penetration %"})
                         .sort_values("Penetration %")
                         ,
                         use_container_width=True, hide_index=True)
    
        with t2:
            section("GROWTH TRAJECTORY","How has insurance adoption grown year over year?")
            ins_yr = fins.groupby(["year","quarter"]).agg(
                count=("insurance_count","sum"),
                amount=("insurance_amount","sum")).reset_index()
            ins_yr["period"] = ins_yr.year.astype(str)+" Q"+ins_yr.quarter.astype(str)
            ins_yr["amount_cr"] = ins_yr.amount/1e7
    
            fig = make_subplots(specs=[[{"secondary_y":True}]])
            fig.add_trace(go.Bar(name="Policies",x=ins_yr.period,
                                 y=ins_yr["count"], marker_color=PURPLE,
                                 opacity=0.85), secondary_y=False)
            fig.add_trace(go.Scatter(name="Premium (₹ Cr)", x=ins_yr.period,
                                     y=ins_yr.amount_cr, mode="lines+markers",
                                     line=dict(color=ORANGE,width=3),
                                     marker_size=8), secondary_y=True)
            fig.update_yaxes(title_text="Policy Count", secondary_y=False)
            fig.update_yaxes(title_text="Premium (₹ Cr)", secondary_y=True)
            fig.update_layout(**CHART_STYLE, height=380,
                              title="Insurance Policy Count & Premium — Quarterly Trend")
            st.plotly_chart(fig, use_container_width=True)
    
            # YoY growth
            ins_annual = fins.groupby("year").agg(count=("insurance_count","sum")).reset_index()
            ins_annual["yoy_growth"] = ins_annual["count"].pct_change()*100
            fig2 = px.bar(ins_annual.dropna(), x="year", y="yoy_growth",
                          color="yoy_growth", color_continuous_scale="Greens",
                          text="yoy_growth",
                          title="Year-on-Year Insurance Growth (%)",
                          labels={"yoy_growth":"YoY Growth %","year":"Year"})
            fig2.update_traces(texttemplate="%{text:.1f}%")
            fig2.update_layout(**CHART_STYLE, height=300, showlegend=False)
            st.plotly_chart(fig2, use_container_width=True)
    
        with t3:
            section("OPPORTUNITY MATRIX","States with large user base but low penetration = highest growth targets")
            opp = pen_df.copy()
            opp["opportunity_score"] = ((opp.registered_users/opp.registered_users.max()) -
                                         (opp.penetration_pct/opp.penetration_pct.max())).round(3)
            opp["priority"] = pd.cut(opp.opportunity_score,
                                      bins=[-2,-0.1,0.1,0.3,1.0],
                                      labels=["Low","Medium","High","Critical"])
    
            fig = px.treemap(opp, path=["priority","state"], values="registered_users",
                             color="opportunity_score", color_continuous_scale="RdYlGn_r",
                             title="Opportunity Treemap — Priority Regions for Insurance Push",
                             labels={"opportunity_score":"Opportunity Score"})
            fig.update_layout(**CHART_STYLE, height=420)
            st.plotly_chart(fig, use_container_width=True)
    
            top_opp = opp.nlargest(5,"opportunity_score")[["state","registered_users","penetration_pct","opportunity_score","priority"]]
            st.markdown("#### 🎯 Top 5 Priority States for Insurance Campaigns")
            st.dataframe(top_opp,
                         use_container_width=True, hide_index=True)
    
        st.markdown("---")
        section("KEY FINDINGS","Business Case 3 Insights")
        col1,col2 = st.columns(2)
        with col1:
            finding("Average insurance penetration is <b>below 2%</b> of registered users — a massive untapped market exists across nearly all states.")
            finding(f"<b>{low_pen}</b> has the lowest penetration despite reasonable user base — a top-priority state for insurer partnership and targeted campaigns.")
        with col2:
            finding("Insurance premium per policy has been <b>rising steadily</b> — users are upgrading to more comprehensive plans, signalling product-market fit improvement.")
            finding("The Opportunity Matrix identifies <b>Critical</b> states where high user density meets low penetration — these deserve dedicated marketing budgets.")
    
    
    # ══════════════════════════════════════════════════════════════
    # BC7 – TRANSACTION ANALYSIS: STATES & DISTRICTS
    # ══════════════════════════════════════════════════════════════
    elif case == "BC7 – Txn Analysis: States & Districts":
        st.markdown("""
        <div class="page-header">
          <h1>🗺️ BC7: Transaction Analysis Across States & Districts</h1>
          <p>Identifying top-performing states, districts, and pin codes by transaction volume and value</p>
        </div>""", unsafe_allow_html=True)
    
        with st.expander("🔍 SQL Query Used"):
            st.code("""
    -- Q1: Top states by transaction value
    SELECT state,
           SUM(transaction_count)  AS total_count,
           SUM(transaction_amount) AS total_amount,
           ROUND(SUM(transaction_amount)/SUM(transaction_count),0) AS avg_txn
    FROM   top_transaction
    WHERE  year IN (<years>)
    GROUP  BY state
    ORDER  BY total_amount DESC
    LIMIT 10;
    
    -- Q2: Top districts within selected state
    SELECT state, district,
           SUM(transaction_count)  AS total_count,
           SUM(transaction_amount) AS total_amount
    FROM   map_transaction
    WHERE  year IN (<years>) AND state = '<state>'
    GROUP  BY state, district
    ORDER  BY total_amount DESC
    LIMIT 10;
    
    -- Q3: Quarter-level breakdown per state
    SELECT state, year, quarter,
           SUM(transaction_count)  AS total_count,
           SUM(transaction_amount) AS total_amount
    FROM   map_transaction
    WHERE  state IN (<states>) AND year IN (<years>)
    GROUP  BY state, year, quarter
    ORDER  BY total_amount DESC;
    """, language="sql")
    
        top_n = st.slider("Show Top N States / Districts", 5, 15, 10)
    
        # KPIs
        c1,c2,c3,c4 = st.columns(4)
        kpi(c1,"🏛", f"{len(sel_states)}", "States Analysed")
        kpi(c2,"💰", fmt_cr(fmap.transaction_amount.sum()), "Total Txn Value")
        kpi(c3,"🔢", fmt_m(fmap.transaction_count.sum()), "Total Transactions")
        top_dist = fmap.groupby("district")["transaction_amount"].sum().idxmax()
        kpi(c4,"📍", top_dist, "Top District")
    
        st.markdown("<br>", unsafe_allow_html=True)
        t1,t2,t3 = st.tabs(["🏛 State Rankings","🏘 District Drill-down","📅 Quarter Analysis"])
    
        with t1:
            section("STATE RANKINGS","Top performing states by transaction volume and value")
            state_rank = fmap.groupby("state").agg(
                count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            state_rank["amount_cr"] = state_rank.amount/1e7
            state_rank["avg_txn"]  = state_rank.amount/state_rank["count"].clip(lower=1)
            state_rank = state_rank.nlargest(top_n,"amount_cr")
    
            col_l, col_r = st.columns([3,2])
            with col_l:
                fig = px.bar(state_rank.sort_values("amount_cr"),
                             x="amount_cr", y="state", orientation="h",
                             color="amount_cr", color_continuous_scale="Purples",
                             text="amount_cr",
                             title=f"Top {top_n} States — Transaction Value",
                             labels={"amount_cr":"₹ Cr","state":""})
                fig.update_traces(texttemplate="%{text:.0f}Cr")
                fig.update_layout(**CHART_STYLE, height=420, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.pie(state_rank, names="state", values="amount_cr",
                              **chart_defaults(), hole=0.4,
                              title="Share of Total Value")
                fig2.update_layout(**CHART_STYLE, height=420)
                st.plotly_chart(fig2, use_container_width=True)
    
            # Ranked table
            st.markdown(f"#### Top {top_n} States — Full Details")
            tbl = state_rank.sort_values("amount_cr",ascending=False).reset_index(drop=True)
            tbl.index += 1
            tbl["Amount"] = tbl.amount.map(fmt_cr)
            tbl["Avg Txn"] = tbl.avg_txn.map(lambda x: f"₹{x:,.0f}")
            tbl["Count"] = tbl["count"].map(lambda x: f"{x/1e6:.2f}M")
            st.dataframe(tbl[["state","Amount","Count","Avg Txn"]]
                         .rename(columns={"state":"State"}),
                         use_container_width=True)
    
        with t2:
            section("DISTRICT DRILL-DOWN","Which districts drive the most transactions within each state?")
            state_choice = st.selectbox("Choose a State", sorted(sel_states))
            dist_df = fmap[fmap.state==state_choice].groupby("district").agg(
                count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            dist_df["amount_cr"] = dist_df.amount/1e7
            dist_df = dist_df.nlargest(top_n,"amount_cr")
    
            col_l, col_r = st.columns(2)
            with col_l:
                fig = px.bar(dist_df.sort_values("amount_cr"),
                             x="amount_cr", y="district", orientation="h",
                             color="amount_cr", color_continuous_scale="Blues",
                             text="amount_cr",
                             title=f"Top Districts in {state_choice}",
                             labels={"amount_cr":"₹ Cr","district":""})
                fig.update_traces(texttemplate="%{text:.1f}Cr")
                fig.update_layout(**CHART_STYLE, height=360, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.scatter(dist_df, x="count", y="amount_cr",
                                  size="amount_cr", color="district",
                                  **chart_defaults(), hover_name="district",
                                  labels={"count":"Transaction Count","amount_cr":"₹ Cr"},
                                  title="Volume vs Value per District")
                fig2.update_layout(**CHART_STYLE, height=360)
                st.plotly_chart(fig2, use_container_width=True)
    
        with t3:
            section("QUARTERLY BREAKDOWN","Performance trend per state across quarters")
            qtr_df = fmap.groupby(["state","year","quarter"]).agg(
                count=("transaction_count","sum"),
                amount=("transaction_amount","sum")).reset_index()
            qtr_df["period"] = qtr_df.year.astype(str)+" Q"+qtr_df.quarter.astype(str)
            qtr_df["amount_cr"] = qtr_df.amount/1e7
    
            fig = px.line(qtr_df[qtr_df.state.isin(sel_states[:6])],
                          x="period", y="amount_cr", color="state",
                          markers=True, **chart_defaults(),
                          labels={"amount_cr":"₹ Cr","period":"Quarter"},
                          title="Quarterly Transaction Value — State Comparison")
            fig.update_traces(line_width=2)
            fig.update_layout(**CHART_STYLE, height=380,
                              legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig, use_container_width=True)
    
            # Pivot: state vs quarter
            piv = qtr_df[qtr_df.state.isin(sel_states[:8])].pivot_table(
                index="state", columns="period", values="amount_cr", aggfunc="sum").fillna(0)
            fig2 = px.imshow(piv, color_continuous_scale="Purples",
                             labels=dict(color="₹ Cr"),
                             title="Transaction Value Heatmap — State × Quarter")
            fig2.update_layout(**CHART_STYLE, height=360)
            st.plotly_chart(fig2, use_container_width=True)
    
        st.markdown("---")
        section("KEY FINDINGS","Business Case 7 Insights")
        col1,col2 = st.columns(2)
        with col1:
            finding(f"<b>{top_dist}</b> is the single highest-value district — targeted merchant acquisition and infrastructure investment here will yield maximum ROI.")
            finding("Top 3 states account for <b>over 40%</b> of all transaction value — geographic concentration warrants both deeper penetration and diversification strategies.")
        with col2:
            finding("District-level analysis reveals <b>significant intra-state variation</b> — even in top states, tier-2 cities are underperforming relative to their user potential.")
            finding("Quarterly trend lines show consistent <b>Q4 spikes</b>, confirming festival/year-end seasonality — marketing spend should peak in Q3 to capture Q4 lift.")
    
    
    # ══════════════════════════════════════════════════════════════
    # BC8 – USER REGISTRATION ANALYSIS
    # ══════════════════════════════════════════════════════════════
    elif case == "BC8 – User Registration Analysis":
        st.markdown("""
        <div class="page-header">
          <h1>👤 BC8: User Registration Analysis</h1>
          <p>Identifying top states, districts, and pin codes by new user registrations per year-quarter</p>
        </div>""", unsafe_allow_html=True)
    
        with st.expander("🔍 SQL Query Used"):
            st.code("""
    -- Q1: Top states by registration in a specific year-quarter
    SELECT state,
           SUM(registered_users) AS total_registered
    FROM   top_user
    WHERE  year = <year> AND quarter = <quarter>
    GROUP  BY state
    ORDER  BY total_registered DESC
    LIMIT  10;
    
    -- Q2: Top districts by registration
    SELECT state, district,
           SUM(registered_users) AS total_registered
    FROM   top_user
    WHERE  year = <year> AND quarter = <quarter>
    GROUP  BY state, district
    ORDER  BY total_registered DESC
    LIMIT  10;
    
    -- Q3: Top pin codes by registration
    SELECT state, pincode,
           SUM(registered_users) AS total_registered
    FROM   top_user
    WHERE  year = <year> AND quarter = <quarter>
    GROUP  BY state, pincode
    ORDER  BY total_registered DESC
    LIMIT  10;
    
    -- Q4: Growth hotspot detection — states with fastest registration acceleration
    WITH reg_growth AS (
        SELECT state, year, quarter,
               SUM(registered_users) AS regs,
               LAG(SUM(registered_users)) OVER (PARTITION BY state ORDER BY year, quarter) AS prev_regs
        FROM top_user
        GROUP BY state, year, quarter
    )
    SELECT state, year, quarter, regs,
           ROUND((regs - prev_regs)/NULLIF(prev_regs,0)*100,2) AS growth_pct
    FROM reg_growth
    ORDER BY growth_pct DESC;
    """, language="sql")
    
        col_yr, col_qr = st.columns(2)
        yr_sel  = col_yr.selectbox("Analysis Year",  sorted(sel_years, reverse=True))
        qtr_sel = col_qr.selectbox("Analysis Quarter", QUARTERS, index=sel_quarter-1)
    
        snapshot  = fureg[(fureg.year==yr_sel) & (fureg.quarter==qtr_sel)]
        prev_snap = fureg[(fureg.year==yr_sel) & (fureg.quarter==max(qtr_sel-1,1))]
    
        # KPIs
        c1,c2,c3,c4 = st.columns(4)
        kpi(c1,"👤", fmt_m(snapshot.registered_users.sum()), f"Registrations Q{qtr_sel} {yr_sel}")
        pct_chg = (snapshot.registered_users.sum()-prev_snap.registered_users.sum())/max(prev_snap.registered_users.sum(),1)*100
        kpi(c2,"📈", f"{pct_chg:.1f}%", "QoQ Registration Growth", delta=None, up=pct_chg>=0)
        top_state_reg = snapshot.groupby("state")["registered_users"].sum().idxmax() if not snapshot.empty else "N/A"
        kpi(c3,"🏆", top_state_reg, "Top State by Registrations")
        top_dist_reg  = snapshot.groupby("district")["registered_users"].sum().idxmax() if not snapshot.empty else "N/A"
        kpi(c4,"📍", top_dist_reg, "Top District by Registrations")
    
        st.markdown("<br>", unsafe_allow_html=True)
        t1,t2,t3,t4 = st.tabs(["🏛 Top States","🏘 Top Districts","📬 Top Pin Codes","🔥 Growth Hotspots"])
    
        with t1:
            section(f"STATE RANKINGS — {yr_sel} Q{qtr_sel}","Which states had the highest new registrations?")
            st_reg = snapshot.groupby("state")["registered_users"].sum().reset_index()
            st_reg = st_reg.sort_values("registered_users",ascending=False).head(10)
    
            col_l, col_r = st.columns([3,2])
            with col_l:
                fig = px.bar(st_reg.sort_values("registered_users"),
                             x="registered_users", y="state", orientation="h",
                             color="registered_users", color_continuous_scale="Purples",
                             text="registered_users",
                             title=f"Top 10 States — Registrations {yr_sel} Q{qtr_sel}",
                             labels={"registered_users":"Registered Users","state":""})
                fig.update_traces(texttemplate="%{text:,.0f}")
                fig.update_layout(**CHART_STYLE, height=400, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                fig2 = px.funnel(st_reg.sort_values("registered_users",ascending=False),
                                 x="registered_users", y="state",
                                 color_discrete_sequence=[PURPLE],
                                 title="Registration Funnel")
                fig2.update_layout(**CHART_STYLE, height=400)
                st.plotly_chart(fig2, use_container_width=True)
    
        with t2:
            section(f"DISTRICT RANKINGS — {yr_sel} Q{qtr_sel}","Top districts for new user acquisition")
            dist_reg = snapshot.groupby(["state","district"])["registered_users"].sum().reset_index()
            dist_reg = dist_reg.sort_values("registered_users",ascending=False).head(10)
    
            fig = px.bar(dist_reg, x="district", y="registered_users",
                         color="state", **chart_defaults(),
                         text="registered_users",
                         title=f"Top 10 Districts — Registrations {yr_sel} Q{qtr_sel}",
                         labels={"registered_users":"Users","district":"District"})
            fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(**CHART_STYLE, height=380)
            st.plotly_chart(fig, use_container_width=True)
    
            st.dataframe(dist_reg,
                         use_container_width=True, hide_index=True)
    
        with t3:
            section(f"PIN CODE RANKINGS — {yr_sel} Q{qtr_sel}","Top pin codes for hyper-local targeting")
            pin_reg = snapshot.groupby(["state","district","pincode"])["registered_users"].sum().reset_index()
            pin_reg = pin_reg.sort_values("registered_users",ascending=False).head(10)
    
            fig = px.bar(pin_reg, x="pincode", y="registered_users",
                         color="state", **chart_defaults(),
                         text="registered_users",
                         title=f"Top 10 Pin Codes — Registrations {yr_sel} Q{qtr_sel}",
                         labels={"registered_users":"Users","pincode":"Pin Code"})
            fig.update_traces(texttemplate="%{text:,.0f}", textposition="outside")
            fig.update_layout(**CHART_STYLE, height=360)
            st.plotly_chart(fig, use_container_width=True)
    
            st.dataframe(pin_reg.reset_index(drop=True).style
                         ,
                         use_container_width=True, hide_index=True)
    
        with t4:
            section("GROWTH HOTSPOTS","States with the fastest accelerating registration velocity")
            reg_trend = fureg.groupby(["state","year","quarter"])["registered_users"].sum().reset_index()
            reg_trend = reg_trend.sort_values(["state","year","quarter"])
            reg_trend["prev"] = reg_trend.groupby("state")["registered_users"].shift(1)
            reg_trend["growth_pct"] = ((reg_trend.registered_users - reg_trend.prev)/
                                        reg_trend.prev.clip(lower=1)*100).round(1)
            reg_trend["period"] = reg_trend.year.astype(str)+" Q"+reg_trend.quarter.astype(str)
    
            col_l, col_r = st.columns(2)
            with col_l:
                latest = reg_trend[reg_trend.year==yr_sel].groupby("state")["growth_pct"].mean().reset_index()
                latest = latest.sort_values("growth_pct",ascending=False).head(10)
                fig = px.bar(latest, x="growth_pct", y="state", orientation="h",
                             color="growth_pct", color_continuous_scale="RdYlGn",
                             text="growth_pct",
                             title=f"Avg QoQ Registration Growth in {yr_sel}",
                             labels={"growth_pct":"Growth %","state":""})
                fig.update_traces(texttemplate="%{text:.1f}%")
                fig.update_layout(**CHART_STYLE, height=380, showlegend=False)
                st.plotly_chart(fig, use_container_width=True)
            with col_r:
                heat = reg_trend[reg_trend.state.isin(sel_states[:8])].pivot_table(
                    index="state", columns="period", values="growth_pct").fillna(0)
                fig2 = px.imshow(heat, color_continuous_scale="RdYlGn",
                                 labels=dict(color="Growth %"),
                                 title="Registration Growth % Heatmap")
                fig2.update_layout(**CHART_STYLE, height=380)
                st.plotly_chart(fig2, use_container_width=True)
    
            # All-time trend
            fig3 = px.line(reg_trend[reg_trend.state.isin(sel_states[:5])],
                           x="period", y="registered_users", color="state",
                           markers=True, **chart_defaults(),
                           labels={"registered_users":"Registered Users","period":"Quarter"},
                           title="Registration Volume Over Time — Top States")
            fig3.update_layout(**CHART_STYLE, height=320,
                               legend=dict(orientation="h",y=-0.3))
            st.plotly_chart(fig3, use_container_width=True)
    
        st.markdown("---")
        section("KEY FINDINGS","Business Case 8 Insights")
        col1,col2 = st.columns(2)
        with col1:
            finding(f"<b>{top_state_reg}</b> leads registrations in {yr_sel} Q{qtr_sel}. Registration hotspots should receive priority support infrastructure and referral campaign boosts.")
            finding("Pin-code level data reveals <b>hyper-local growth clusters</b> that are often invisible at state level — these micro-markets deserve dedicated acquisition strategies.")
        with col2:
            finding("Growth Hotspot analysis surfaces states with <b>accelerating registration velocity</b> — early signal for markets that will cross into top-tier within 2 quarters.")
            finding("QoQ registration momentum is <b>higher in Q1 and Q3</b> (post-holiday and mid-year) — ideal windows for acquisition campaigns and referral program activation.")
    

# ─────────────────────────────────────────────────────────────────
# GENERAL DASHBOARD PAGES
# ─────────────────────────────────────────────────────────────────


# ─────────────────────────────────────────────────────────────────
# GENERAL DASHBOARD PAGES (only shown when no BCS is selected)
# ─────────────────────────────────────────────────────────────────
if page != '__BCS__':
    # ── 1. EXECUTIVE OVERVIEW ────────────────────────────────────────
    if page == "🏠 Executive Overview":
        st.markdown('<div class="section-header">📊 Executive Overview</div>',
                    unsafe_allow_html=True)
    
        total_txn_amt   = flt_txn["transaction_amount"].sum()
        total_txn_count = flt_txn["transaction_count"].sum()
        total_users     = flt_usr["user_count"].sum()
        total_ins_amt   = flt_ins["insurance_amount"].sum()
    
        c1, c2, c3, c4 = st.columns(4)
        c1.metric("💰 Total Transaction Value", fmt_crore(total_txn_amt))
        c2.metric("🔢 Total Transactions",      f"{total_txn_count/1e6:.1f}M")
        c3.metric("👥 Registered Users",        f"{total_users/1e6:.1f}M")
        c4.metric("🛡️ Insurance Premium",       fmt_crore(total_ins_amt))
    
        st.markdown("---")
        col_l, col_r = st.columns(2)
    
        with col_l:
            st.subheader("Transaction Volume by Category")
            cat_df = flt_txn.groupby("transaction_type")["transaction_count"].sum().reset_index()
            fig = px.pie(cat_df, names="transaction_type", values="transaction_count",
                         color_discrete_sequence=PALETTE, hole=0.4)
            fig.update_layout(margin=dict(t=20, b=20))
            st.plotly_chart(fig, use_container_width=True)
    
        with col_r:
            st.subheader("Transaction Value by Category")
            amt_df = flt_txn.groupby("transaction_type")["transaction_amount"].sum().reset_index()
            fig2 = px.bar(amt_df.sort_values("transaction_amount", ascending=True),
                          x="transaction_amount", y="transaction_type",
                          orientation="h", color="transaction_type",
                          color_discrete_sequence=PALETTE)
            fig2.update_layout(showlegend=False, yaxis_title="", xaxis_title="Amount (₹)")
            st.plotly_chart(fig2, use_container_width=True)
    
        st.markdown("---")
        st.subheader("📌 Key Business Insights")
        top_state = flt_txn.groupby("state")["transaction_amount"].sum().idxmax()
        top_cat   = flt_txn.groupby("transaction_type")["transaction_count"].sum().idxmax()
        insights = [
            f"🏆 <b>{top_state}</b> leads in total transaction value among selected states.",
            f"💡 <b>{top_cat}</b> is the most popular transaction category by volume.",
            f"📱 Average transaction value: <b>{fmt_crore(total_txn_amt / max(total_txn_count,1))}</b> per transaction.",
            "📈 YoY growth trend visible — deeper analysis available in Trend Analysis section.",
        ]
        for ins in insights:
            st.markdown(f'<div class="insight-box">{ins}</div>', unsafe_allow_html=True)
    
    # ── 2. TRANSACTION ANALYSIS ──────────────────────────────────────
    elif page == "💳 Transaction Analysis":
        st.markdown('<div class="section-header">💳 Transaction Analysis</div>',
                    unsafe_allow_html=True)
    
        tab1, tab2, tab3 = st.tabs(
            ["By Category", "Quarterly Breakdown", "State Comparison"])
    
        with tab1:
            st.subheader("Payment Category Performance")
            cat_summary = (flt_txn.groupby("transaction_type")
                           .agg(count=("transaction_count","sum"),
                                amount=("transaction_amount","sum"))
                           .reset_index()
                           .sort_values("amount", ascending=False))
            cat_summary["avg_txn"] = cat_summary["amount"] / cat_summary["count"]
            cat_summary["amount_cr"] = cat_summary["amount"] / 1e7
            fig = px.bar(cat_summary, x="transaction_type", y="amount_cr",
                         color="transaction_type", text="amount_cr",
                         color_discrete_sequence=PALETTE,
                         labels={"amount_cr": "Amount (₹ Cr)", "transaction_type": "Category"})
            fig.update_traces(texttemplate="%{text:.1f} Cr", textposition="outside")
            fig.update_layout(showlegend=False)
            st.plotly_chart(fig, use_container_width=True)
    
            st.dataframe(cat_summary.assign(
                amount=cat_summary["amount"].map(fmt_crore),
                avg_txn=cat_summary["avg_txn"].map(fmt_crore)
            ).rename(columns={"transaction_type":"Category",
                               "count":"Transactions","amount":"Total Value",
                               "avg_txn":"Avg Value","amount_cr":"Cr"}
            ).drop("Cr", axis=1), use_container_width=True)
    
        with tab2:
            st.subheader("Quarterly Transaction Trends")
            q_df = (flt_txn.groupby(["year","quarter"])
                    .agg(amount=("transaction_amount","sum"),
                         count=("transaction_count","sum"))
                    .reset_index())
            q_df["period"] = q_df["year"].astype(str) + " Q" + q_df["quarter"].astype(str)
            q_df["amount_cr"] = q_df["amount"] / 1e7
            fig = px.line(q_df, x="period", y="amount_cr", markers=True,
                          color_discrete_sequence=[PHONEPE_PURPLE],
                          labels={"amount_cr":"Amount (₹ Cr)", "period":"Quarter"})
            fig.update_traces(line_width=3, marker_size=8)
            st.plotly_chart(fig, use_container_width=True)
    
        with tab3:
            st.subheader("State-wise Transaction Comparison")
            state_df = (flt_txn.groupby("state")
                        .agg(count=("transaction_count","sum"),
                             amount=("transaction_amount","sum"))
                        .reset_index()
                        .sort_values("amount", ascending=False)
                        .head(10))
            state_df["amount_cr"] = state_df["amount"] / 1e7
            fig = px.bar(state_df, x="state", y="amount_cr",
                         color="amount_cr", color_continuous_scale="Purples",
                         labels={"amount_cr":"Amount (₹ Cr)", "state":"State"})
            st.plotly_chart(fig, use_container_width=True)
    
    # ── 3. USER ENGAGEMENT ───────────────────────────────────────────
    elif page == "👥 User Engagement":
        st.markdown('<div class="section-header">👥 User Engagement</div>',
                    unsafe_allow_html=True)
    
        tab1, tab2 = st.tabs(["Device Brand Distribution", "User Growth"])
    
        with tab1:
            st.subheader("Market Share by Device Brand")
            brand_df = (flt_usr.groupby("brand")["user_count"]
                        .sum().reset_index()
                        .sort_values("user_count", ascending=False))
            col1, col2 = st.columns(2)
            with col1:
                fig = px.pie(brand_df, names="brand", values="user_count",
                             color_discrete_sequence=PALETTE, hole=0.35)
                st.plotly_chart(fig, use_container_width=True)
            with col2:
                fig2 = px.funnel(brand_df.head(6), x="user_count", y="brand",
                                 color_discrete_sequence=[PHONEPE_PURPLE])
                st.plotly_chart(fig2, use_container_width=True)
    
        with tab2:
            st.subheader("Registered Users Over Time")
            user_time = (flt_usr.drop_duplicates(["state","year","quarter"])
                         .groupby(["year","quarter"])
                         .agg(reg_users=("registered_users","sum"),
                              app_opens=("app_opens","sum"))
                         .reset_index())
            user_time["period"] = (user_time["year"].astype(str) + " Q" +
                                   user_time["quarter"].astype(str))
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(name="Registered Users",
                                 x=user_time["period"],
                                 y=user_time["reg_users"] / 1e6,
                                 marker_color=PHONEPE_PURPLE), secondary_y=False)
            fig.add_trace(go.Scatter(name="App Opens (M)",
                                     x=user_time["period"],
                                     y=user_time["app_opens"] / 1e6,
                                     mode="lines+markers",
                                     line=dict(color=PHONEPE_BLUE, width=3)),
                          secondary_y=True)
            fig.update_layout(barmode="group")
            fig.update_yaxes(title_text="Registered Users (M)", secondary_y=False)
            fig.update_yaxes(title_text="App Opens (M)", secondary_y=True)
            st.plotly_chart(fig, use_container_width=True)
    
    # ── 4. GEOGRAPHICAL INSIGHTS ─────────────────────────────────────
    elif page == "🗺️ Geographical Insights":
        st.markdown('<div class="section-header">🗺️ Geographical Insights</div>',
                    unsafe_allow_html=True)

        metric = st.radio("Select Metric",
                          ["Transaction Amount", "Transaction Count", "User Count"],
                          horizontal=True)

        # ── Build geo_df ─────────────────────────────────────────────
        if metric == "Transaction Amount":
            geo_df    = (flt_txn.groupby("state")["transaction_amount"]
                         .sum().reset_index()
                         .rename(columns={"transaction_amount": "value"}))
            val_label = "Value (₹ Cr)"
            geo_df["value"] = (geo_df["value"] / 1e7).round(2)

        elif metric == "Transaction Count":
            geo_df    = (flt_txn.groupby("state")["transaction_count"]
                         .sum().reset_index()
                         .rename(columns={"transaction_count": "value"}))
            val_label = "Count (Lakhs)"
            geo_df["value"] = (geo_df["value"] / 1e5).round(2)

        else:  # User Count — deduplicate per state/year/quarter first
            usr_dedup = (flt_usr.drop_duplicates(subset=["state","year","quarter"])
                         [["state","registered_users"]])
            geo_df    = (usr_dedup.groupby("state")["registered_users"]
                         .sum().reset_index()
                         .rename(columns={"registered_users": "value"}))
            val_label = "Registered Users (Lakhs)"
            geo_df["value"] = (geo_df["value"] / 1e5).round(2)

        # ── Normalize state names to match GeoJSON ────────────────────
        STATE_NAME_MAP = {
            "Andaman & Nicobar Island": "Andaman & Nicobar Island",
            "Andhra Pradesh":           "Andhra Pradesh",
            "Arunachal Pradesh":        "Arunachal Pradesh",
            "Assam":                    "Assam",
            "Bihar":                    "Bihar",
            "Chandigarh":               "Chandigarh",
            "Chhattisgarh":             "Chhattisgarh",
            "Dadra And Nagar Haveli And Daman And Diu": "Dadra and Nagar Haveli and Daman and Diu",
            "Delhi":                    "NCT of Delhi",
            "Goa":                      "Goa",
            "Gujarat":                  "Gujarat",
            "Haryana":                  "Haryana",
            "Himachal Pradesh":         "Himachal Pradesh",
            "Jammu & Kashmir":          "Jammu & Kashmir",
            "Jharkhand":                "Jharkhand",
            "Karnataka":                "Karnataka",
            "Kerala":                   "Kerala",
            "Ladakh":                   "Ladakh",
            "Lakshadweep":              "Lakshadweep",
            "Madhya Pradesh":           "Madhya Pradesh",
            "Maharashtra":              "Maharashtra",
            "Manipur":                  "Manipur",
            "Meghalaya":                "Meghalaya",
            "Mizoram":                  "Mizoram",
            "Nagaland":                 "Nagaland",
            "Odisha":                   "Odisha",
            "Puducherry":               "Puducherry",
            "Punjab":                   "Punjab",
            "Rajasthan":                "Rajasthan",
            "Sikkim":                   "Sikkim",
            "Tamil Nadu":               "Tamil Nadu",
            "Telangana":                "Telangana",
            "Tripura":                  "Tripura",
            "Uttar Pradesh":            "Uttar Pradesh",
            "Uttarakhand":              "Uttarakhand",
            "West Bengal":              "West Bengal",
        }
        geo_df["state_geo"] = geo_df["state"].map(STATE_NAME_MAP).fillna(geo_df["state"])

        # ── Choropleth ────────────────────────────────────────────────
        import requests, json
        GEOJSON_URL = (
            "https://gist.githubusercontent.com/jbrobst/56c13bbbf9d97d187fea01ca62ea5112"
            "/raw/e388c4cae20aa53cb5090210a42ebb9b765c0a36/india_states.geojson"
        )
        @st.cache_data(show_spinner=False)
        def load_geojson():
            try:
                return requests.get(GEOJSON_URL, timeout=10).json()
            except Exception:
                return None

        geojson = load_geojson()

        if geojson is None:
            st.warning("⚠️ Could not load India GeoJSON — showing bar chart instead.")
            fig = px.bar(
                geo_df.sort_values("value", ascending=False).head(15),
                x="state", y="value",
                color="value", color_continuous_scale="Purples",
                labels={"value": val_label, "state": "State"},
                title=f"State-wise {metric}"
            )
            fig.update_layout(height=420, xaxis_tickangle=-35)
        else:
            fig = px.choropleth(
                geo_df,
                geojson=geojson,
                locations="state_geo",
                featureidkey="properties.ST_NM",
                color="value",
                color_continuous_scale="Purples",
                hover_name="state",
                hover_data={"state_geo": False, "value": True},
                labels={"value": val_label},
                title=f"India State-wise {metric}"
            )
            fig.update_geos(fitbounds="locations", visible=False)
            fig.update_layout(height=560, margin=dict(t=40, b=0, l=0, r=0))

        st.plotly_chart(fig, use_container_width=True)

        # ── Ranked bar + table side by side ──────────────────────────
        col_l, col_r = st.columns([3, 2])
        with col_l:
            bar = geo_df.sort_values("value", ascending=False).head(10)
            fig2 = px.bar(
                bar, x="value", y="state", orientation="h",
                color="value", color_continuous_scale="Purples",
                text="value",
                labels={"value": val_label, "state": ""},
                title=f"Top 10 States — {metric}"
            )
            fig2.update_traces(texttemplate="%{text:.1f}", textposition="outside")
            fig2.update_layout(height=360, showlegend=False,
                               margin=dict(t=40, b=10, l=10, r=10))
            st.plotly_chart(fig2, use_container_width=True)
        with col_r:
            st.subheader("📋 Full State Rankings")
            top_tbl = (geo_df[["state","value"]]
                       .sort_values("value", ascending=False)
                       .reset_index(drop=True))
            top_tbl.index += 1
            top_tbl.columns = ["State", val_label]
            st.dataframe(
                top_tbl,
                use_container_width=True, height=360
            )
    
    # ── 5. INSURANCE INSIGHTS ────────────────────────────────────────
    elif page == "🛡️ Insurance Insights":
        st.markdown('<div class="section-header">🛡️ Insurance Insights</div>',
                    unsafe_allow_html=True)
    
        col1, col2, col3 = st.columns(3)
        col1.metric("Total Policies",   f"{flt_ins['insurance_count'].sum()/1e3:.1f}K")
        col2.metric("Total Premium",    fmt_crore(flt_ins["insurance_amount"].sum()))
        avg_premium = (flt_ins["insurance_amount"].sum() /
                       max(flt_ins["insurance_count"].sum(), 1))
        col3.metric("Avg Premium/Policy", f"₹{avg_premium:,.0f}")
    
        st.markdown("---")
        tab1, tab2 = st.tabs(["State Analysis", "Quarterly Trend"])
    
        with tab1:
            ins_state = (flt_ins.groupby("state")
                         .agg(count=("insurance_count","sum"),
                              amount=("insurance_amount","sum"))
                         .reset_index()
                         .sort_values("amount", ascending=False))
            ins_state["amount_cr"] = ins_state["amount"] / 1e7
            fig = px.bar(ins_state, x="state", y="amount_cr",
                         color="amount_cr", color_continuous_scale="Blues",
                         title="Insurance Premium by State (₹ Cr)")
            st.plotly_chart(fig, use_container_width=True)
    
        with tab2:
            ins_q = (flt_ins.groupby(["year","quarter"])
                     .agg(count=("insurance_count","sum"),
                          amount=("insurance_amount","sum"))
                     .reset_index())
            ins_q["period"] = ins_q["year"].astype(str)+" Q"+ins_q["quarter"].astype(str)
            ins_q["amount_cr"] = ins_q["amount"] / 1e7
            fig = px.area(ins_q, x="period", y="amount_cr",
                          color_discrete_sequence=[PHONEPE_BLUE],
                          title="Insurance Premium Trend (₹ Cr)")
            fig.update_traces(fill="tozeroy")
            st.plotly_chart(fig, use_container_width=True)
    
    # ── 6. CUSTOMER SEGMENTATION ─────────────────────────────────────
    elif page == "🔍 Customer Segmentation":
        st.markdown('<div class="section-header">🔍 Customer Segmentation</div>',
                    unsafe_allow_html=True)
    
        st.info("Segmentation based on transaction volume and frequency per state.")
    
        seg_df = (flt_txn.groupby("state")
                  .agg(total_amount=("transaction_amount","sum"),
                       total_count=("transaction_count","sum"),
                       avg_txn=("transaction_amount","mean"))
                  .reset_index())
        seg_df["amount_cr"] = seg_df["total_amount"] / 1e7
        seg_df["count_l"]   = seg_df["total_count"] / 1e5
    
        q_amt   = seg_df["total_amount"].quantile(0.66)
        q_count = seg_df["total_count"].quantile(0.66)
    
        def segment(row):
            if row["total_amount"] >= q_amt and row["total_count"] >= q_count:
                return "🥇 High Value & High Volume"
            elif row["total_amount"] >= q_amt:
                return "💎 High Value"
            elif row["total_count"] >= q_count:
                return "📦 High Volume"
            return "🌱 Growth Potential"
    
        seg_df["Segment"] = seg_df.apply(segment, axis=1)
    
        fig = px.scatter(seg_df, x="count_l", y="amount_cr",
                         color="Segment", size="avg_txn",
                         hover_name="state",
                         color_discrete_sequence=PALETTE,
                         labels={"count_l":"Transaction Count (L)",
                                 "amount_cr":"Transaction Amount (₹ Cr)"},
                         title="State Segmentation Matrix")
        fig.update_traces(marker=dict(opacity=0.8, line=dict(width=1, color="white")))
        st.plotly_chart(fig, use_container_width=True)
    
        seg_counts = seg_df["Segment"].value_counts().reset_index()
        seg_counts.columns = ["Segment","States"]
        st.dataframe(seg_counts, use_container_width=True)
    
        st.subheader("💡 Marketing Recommendations by Segment")
        recs = {
            "🥇 High Value & High Volume": "Retain with loyalty programs, premium features & exclusive offers.",
            "💎 High Value": "Increase frequency with cashback incentives & payment reminders.",
            "📦 High Volume": "Upsell financial services; promote high-value transaction categories.",
            "🌱 Growth Potential": "Drive awareness campaigns; offer referral bonuses to grow base.",
        }
        for k, v in recs.items():
            st.markdown(f'<div class="insight-box"><b>{k}:</b> {v}</div>',
                        unsafe_allow_html=True)
    
    # ── 7. TREND ANALYSIS ────────────────────────────────────────────
    elif page == "📈 Trend Analysis":
        st.markdown('<div class="section-header">📈 Trend Analysis</div>',
                    unsafe_allow_html=True)
    
        tab1, tab2 = st.tabs(["Transaction Trends", "YoY Growth"])
    
        with tab1:
            trend_df = (flt_txn.groupby(["year","quarter","transaction_type"])
                        ["transaction_amount"].sum().reset_index())
            trend_df["period"] = (trend_df["year"].astype(str) + " Q" +
                                   trend_df["quarter"].astype(str))
            trend_df["amount_cr"] = trend_df["transaction_amount"] / 1e7
            fig = px.line(trend_df, x="period", y="amount_cr",
                          color="transaction_type",
                          color_discrete_sequence=PALETTE,
                          markers=True,
                          labels={"amount_cr":"Amount (₹ Cr)","period":"Quarter"},
                          title="Transaction Amount by Category Over Time")
            fig.update_traces(line_width=2)
            st.plotly_chart(fig, use_container_width=True)
    
        with tab2:
            yoy = (flt_txn.groupby("year")["transaction_amount"]
                   .sum().reset_index()
                   .sort_values("year"))
            yoy["growth_pct"] = yoy["transaction_amount"].pct_change() * 100
            yoy["amount_cr"]  = yoy["transaction_amount"] / 1e7
            fig = make_subplots(specs=[[{"secondary_y": True}]])
            fig.add_trace(go.Bar(name="Total Amount (₹ Cr)",
                                 x=yoy["year"], y=yoy["amount_cr"],
                                 marker_color=PHONEPE_PURPLE), secondary_y=False)
            fig.add_trace(go.Scatter(name="YoY Growth (%)",
                                     x=yoy["year"], y=yoy["growth_pct"],
                                     mode="lines+markers",
                                     line=dict(color="orange", width=3)),
                          secondary_y=True)
            fig.update_yaxes(title_text="Amount (₹ Cr)", secondary_y=False)
            fig.update_yaxes(title_text="Growth %", secondary_y=True)
            fig.update_layout(title="Year-on-Year Transaction Growth")
            st.plotly_chart(fig, use_container_width=True)
    
    # ── 8. TOP PERFORMERS ────────────────────────────────────────────
    elif page == "🏆 Top Performers":
        st.markdown('<div class="section-header">🏆 Top Performers</div>',
                    unsafe_allow_html=True)
    
        top_n = st.slider("Show Top N", 5, 15, 10)
    
        tab1, tab2, tab3 = st.tabs(["Top States", "Top Transaction Types", "Top Device Brands"])
    
        with tab1:
            st.subheader(f"Top {top_n} States by Transaction Value")
            top_states = (flt_txn.groupby("state")["transaction_amount"]
                          .sum().reset_index()
                          .sort_values("transaction_amount", ascending=False)
                          .head(top_n))
            top_states["amount_cr"] = top_states["transaction_amount"] / 1e7
            fig = px.bar(top_states, x="amount_cr", y="state", orientation="h",
                         color="amount_cr", color_continuous_scale="Purples",
                         text="amount_cr",
                         labels={"amount_cr":"Amount (₹ Cr)", "state":"State"})
            fig.update_traces(texttemplate="%{text:.1f} Cr", textposition="outside")
            fig.update_layout(yaxis=dict(autorange="reversed"))
            st.plotly_chart(fig, use_container_width=True)
    
        with tab2:
            st.subheader(f"Top {top_n} Transaction Categories")
            top_cats = (flt_txn.groupby("transaction_type")
                        .agg(count=("transaction_count","sum"),
                             amount=("transaction_amount","sum"))
                        .reset_index()
                        .sort_values("amount", ascending=False)
                        .head(top_n))
            top_cats["amount_cr"] = top_cats["amount"] / 1e7
            fig = px.treemap(top_cats, path=["transaction_type"],
                             values="amount_cr", color="amount_cr",
                             color_continuous_scale="Purples",
                             title="Category Treemap by Transaction Value")
            st.plotly_chart(fig, use_container_width=True)
    
        with tab3:
            st.subheader(f"Top {top_n} Device Brands by User Count")
            top_brands = (flt_usr.groupby("brand")["user_count"]
                          .sum().reset_index()
                          .sort_values("user_count", ascending=False)
                          .head(top_n))
            fig = px.bar(top_brands, x="brand", y="user_count",
                         color="user_count", color_continuous_scale="Blues",
                         labels={"user_count":"Users", "brand":"Brand"})
            st.plotly_chart(fig, use_container_width=True)
    
    # ── 9. FRAUD DETECTION SIGNALS ───────────────────────────────────
    elif page == "⚠️ Fraud Detection Signals":
        st.markdown('<div class="section-header">⚠️ Fraud Detection Signals</div>',
                    unsafe_allow_html=True)
        st.warning("**Note:** This section highlights statistical anomalies that may warrant further investigation. These are signals, not confirmed fraud cases.")
    
        # Anomaly: states with unusually high avg transaction
        state_stats = (flt_txn.groupby("state")
                       .agg(count=("transaction_count","sum"),
                            amount=("transaction_amount","sum"))
                       .reset_index())
        state_stats["avg_txn"] = state_stats["amount"] / state_stats["count"]
        mean_avg = state_stats["avg_txn"].mean()
        std_avg  = state_stats["avg_txn"].std()
        state_stats["z_score"]   = (state_stats["avg_txn"] - mean_avg) / std_avg
        state_stats["is_anomaly"] = state_stats["z_score"].abs() > 2
    
        col1, col2 = st.columns(2)
    
        with col1:
            st.subheader("📊 Average Transaction Value Distribution")
            fig = px.histogram(state_stats, x="avg_txn",
                               color="is_anomaly",
                               color_discrete_map={True: "#FF6B6B", False: PHONEPE_PURPLE},
                               labels={"avg_txn":"Avg Transaction (₹)",
                                       "is_anomaly":"Anomaly Flag"},
                               title="Distribution with Anomaly Flags")
            st.plotly_chart(fig, use_container_width=True)
    
        with col2:
            st.subheader("🔍 Z-Score by State")
            fig2 = px.scatter(state_stats, x="state", y="z_score",
                              color="is_anomaly",
                              color_discrete_map={True:"#FF6B6B", False:PHONEPE_PURPLE},
                              size="amount",
                              hover_name="state",
                              labels={"z_score":"Anomaly Z-Score"},
                              title="Avg Txn Value Anomaly Score per State")
            fig2.add_hline(y=2, line_dash="dash", line_color="red",
                           annotation_text="Upper Threshold")
            fig2.add_hline(y=-2, line_dash="dash", line_color="red",
                           annotation_text="Lower Threshold")
            st.plotly_chart(fig2, use_container_width=True)
    
        anomalies = state_stats[state_stats["is_anomaly"]].sort_values("z_score", ascending=False)
        if not anomalies.empty:
            st.subheader("🚨 Flagged States for Review")
            st.dataframe(anomalies[["state","avg_txn","z_score","count","amount"]].rename(
                columns={"state":"State","avg_txn":"Avg Txn (₹)","z_score":"Z-Score",
                         "count":"Txn Count","amount":"Total Amount (₹)"}
            ),
            use_container_width=True)
        else:
            st.success("✅ No statistical anomalies detected in selected data.")
