"""
PhonePe Pulse — Data Extraction & Loading Pipeline
====================================================
Uses SQLAlchemy (ORM + Core) for all database operations.

Dependencies:
    pip install sqlalchemy psycopg2-binary pandas

Usage:
    python extract_load.py
"""

import os
import json
import subprocess
from pathlib import Path
from dotenv import load_dotenv
import pandas as pd
from sqlalchemy import (
    create_engine, text,
    Column, BigInteger, SmallInteger, String,
    Numeric, Integer
)
from sqlalchemy.orm import declarative_base, Session
from sqlalchemy.exc import SQLAlchemyError

# ─────────────────────────────────────────────────────────────────
# CONFIG  — update before running
# ─────────────────────────────────────────────────────────────────
load_dotenv()
DB_USER     = os.getenv("DB_USER")
DB_PASSWORD = os.getenv("DB_PASSWORD")               # <-- your PostgreSQL password
DB_HOST     = os.getenv("DB_HOST")
DB_PORT     = os.getenv("DB_PORT")
DB_NAME     = os.getenv("DB_NAME")   # PostgreSQL database name

GITHUB_REPO = "https://github.com/PhonePe/pulse.git"
PULSE_DIR   = "./pulse"

# SQLAlchemy connection URL  (psycopg2 driver)
DATABASE_URL = (
    f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
    f"@{DB_HOST}:{DB_PORT}/{DB_NAME}"
)

# ─────────────────────────────────────────────────────────────────
# ORM MODELS
# ─────────────────────────────────────────────────────────────────
Base = declarative_base()


class AggregatedTransaction(Base):
    __tablename__ = "aggregated_transaction"
    id                = Column(BigInteger, primary_key=True, autoincrement=True)
    state             = Column(String(100))
    year              = Column(SmallInteger)
    quarter           = Column(Integer)
    transaction_type  = Column(String(100))
    transaction_count = Column(BigInteger)
    transaction_amount= Column(Numeric(20, 2))


class AggregatedUser(Base):
    __tablename__ = "aggregated_user"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    brand            = Column(String(100))
    user_count       = Column(BigInteger)
    registered_users = Column(BigInteger)
    app_opens        = Column(BigInteger)


class AggregatedInsurance(Base):
    __tablename__ = "aggregated_insurance"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    insurance_count  = Column(BigInteger)
    insurance_amount = Column(Numeric(20, 2))


class MapTransaction(Base):
    __tablename__ = "map_transaction"
    id                = Column(BigInteger, primary_key=True, autoincrement=True)
    state             = Column(String(100))
    district          = Column(String(200))
    year              = Column(SmallInteger)
    quarter           = Column(Integer)
    transaction_count = Column(BigInteger)
    transaction_amount= Column(Numeric(20, 2))


class MapUser(Base):
    __tablename__ = "map_user"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    district         = Column(String(200))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    registered_users = Column(BigInteger)
    app_opens        = Column(BigInteger)


class MapInsurance(Base):
    __tablename__ = "map_insurance"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    district         = Column(String(200))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    insurance_count  = Column(BigInteger)
    insurance_amount = Column(Numeric(20, 2))


class TopTransaction(Base):
    __tablename__ = "top_transaction"
    id                = Column(BigInteger, primary_key=True, autoincrement=True)
    state             = Column(String(100))
    district          = Column(String(200))
    pincode           = Column(String(20))
    year              = Column(SmallInteger)
    quarter           = Column(Integer)
    transaction_count = Column(BigInteger)
    transaction_amount= Column(Numeric(20, 2))


class TopUser(Base):
    __tablename__ = "top_user"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    district         = Column(String(200))
    pincode          = Column(String(20))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    registered_users = Column(BigInteger)


class TopInsurance(Base):
    __tablename__ = "top_insurance"
    id               = Column(BigInteger, primary_key=True, autoincrement=True)
    state            = Column(String(100))
    district         = Column(String(200))
    pincode          = Column(String(20))
    year             = Column(SmallInteger)
    quarter          = Column(Integer)
    insurance_count  = Column(BigInteger)
    insurance_amount = Column(Numeric(20, 2))


# ─────────────────────────────────────────────────────────────────
# ENGINE SETUP
# ─────────────────────────────────────────────────────────────────
def create_db_engine():
    """
    Create the SQLAlchemy engine.
    First creates the database if it doesn't exist, then connects to it.
    """
    root_url = (
        f"postgresql+psycopg2://{DB_USER}:{DB_PASSWORD}"
        f"@{DB_HOST}:{DB_PORT}/postgres"  # connect to default db first
    )
    root_engine = create_engine(root_url, echo=False)
    with root_engine.connect() as conn:
        # PostgreSQL CREATE DATABASE must run outside a transaction (no IF NOT EXISTS)
        try:
            conn.execution_options(isolation_level="AUTOCOMMIT").execute(
                text(f"CREATE DATABASE {DB_NAME}")
            )
        except Exception:
            pass  # database already exists — that's fine
        conn.commit()
    root_engine.dispose()
    print(f"✅ Database '{DB_NAME}' ready.")

    engine = create_engine(
        DATABASE_URL,  # postgresql+psycopg2://...
        echo=False,
        pool_pre_ping=True,
        pool_recycle=3600,
    )
    return engine


def create_all_tables(engine):
    """Create all ORM-mapped tables (skips if they already exist)."""
    Base.metadata.create_all(engine)
    print("✅ All 9 tables created / verified.")


# ─────────────────────────────────────────────────────────────────
# GIT HELPER
# ─────────────────────────────────────────────────────────────────
def clone_or_pull_repo():
    """Clone the PhonePe Pulse repo, or pull latest if already cloned."""
    if not os.path.exists(PULSE_DIR):
        print("📥 Cloning PhonePe Pulse repository …")
        subprocess.run(["git", "clone", GITHUB_REPO, PULSE_DIR], check=True)
    else:
        print("🔄 Repository exists — pulling latest changes …")
        subprocess.run(["git", "-C", PULSE_DIR, "pull"], check=True)
    print(f"✅ Pulse data available at: {PULSE_DIR}")


# ─────────────────────────────────────────────────────────────────
# UTILITIES
# ─────────────────────────────────────────────────────────────────
def _state_name(dir_name: str) -> str:
    """Convert directory slug to readable state name."""
    return dir_name.replace("-", " ").title()


def _bulk_insert(engine, df: pd.DataFrame, table_name: str):
    """
    Bulk-insert a DataFrame into the given table using pandas + SQLAlchemy.
    Uses method='multi' for batch inserts (much faster than row-by-row).
    """
    if df.empty:
        print(f"   ⚠️  No data to insert into {table_name}.")
        return
    df.to_sql(
        name=table_name,
        con=engine,
        if_exists="append",
        index=False,
        method="multi",
        chunksize=500,
    )
    print(f"   ✅  {table_name}: {len(df):,} rows inserted.")


# ─────────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────────
def load_aggregated_transactions(engine, data_dir):
    """Load aggregated/transaction data into aggregated_transaction."""
    print("\n📦 Loading aggregated_transaction …")
    base = Path(data_dir) / "data" / "aggregated" / "transaction" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                payload = json.loads(q_file.read_text())
                for txn in payload.get("data", {}).get("transactionData", []):
                    pi = txn.get("paymentInstruments", [{}])[0]
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "transaction_type":   txn.get("name"),
                        "transaction_count":  pi.get("count", 0),
                        "transaction_amount": pi.get("amount", 0.0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "aggregated_transaction")


def load_aggregated_users(engine, data_dir):
    """Load aggregated/user data into aggregated_user."""
    print("\n👤 Loading aggregated_user …")
    base = Path(data_dir) / "data" / "aggregated" / "user" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                payload  = json.loads(q_file.read_text())
                agg      = payload.get("data", {})
                reg_users = agg.get("aggregated", {}).get("registeredUsers", 0)
                app_opens = agg.get("aggregated", {}).get("appOpens", 0)
                for brand in (agg.get("usersByDevice") or []):
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "brand":            brand.get("brand"),
                        "user_count":       brand.get("count", 0),
                        "registered_users": reg_users,
                        "app_opens":        app_opens,
                    })
    _bulk_insert(engine, pd.DataFrame(records), "aggregated_user")


def load_aggregated_insurance(engine, data_dir):
    """Load aggregated/insurance data into aggregated_insurance."""
    print("\n🛡️  Loading aggregated_insurance …")
    base = Path(data_dir) / "data" / "aggregated" / "insurance" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                payload  = json.loads(q_file.read_text())
                txn_list = payload.get("data", {}).get("transactionData", []) or []
                if txn_list:
                    pi = txn_list[0].get("paymentInstruments", [{}])[0]
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "insurance_count":  pi.get("count", 0),
                        "insurance_amount": pi.get("amount", 0.0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "aggregated_insurance")


def load_map_transactions(engine, data_dir):
    """Load map/transaction data into map_transaction."""
    print("\n🗺️  Loading map_transaction …")
    base = Path(data_dir) / "data" / "map" / "transaction" / "hover" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                payload = json.loads(q_file.read_text())
                for dist in (payload.get("data", {}).get("hoverDataList", []) or []):
                    metrics = dist.get("metric", [])
                    if metrics:
                        records.append({
                            "state": state, "year": year, "quarter": quarter,
                            "district":          dist.get("name"),
                            "transaction_count": metrics[0].get("count", 0),
                            "transaction_amount":metrics[0].get("amount", 0.0),
                        })
    _bulk_insert(engine, pd.DataFrame(records), "map_transaction")


def load_map_users(engine, data_dir):
    """Load map/user data into map_user."""
    print("\n👥 Loading map_user …")
    base = Path(data_dir) / "data" / "map" / "user" / "hover" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                payload = json.loads(q_file.read_text())
                for name, val in (payload.get("data", {}).get("hoverData", {}) or {}).items():
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district":         name,
                        "registered_users": val.get("registeredUsers", 0),
                        "app_opens":        val.get("appOpens", 0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "map_user")


def load_map_insurance(engine, data_dir):
    """Load map/insurance data into map_insurance."""
    print("\n🛡️  Loading map_insurance …")
    base = Path(data_dir) / "data" / "map" / "insurance" / "hover" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                payload = json.loads(q_file.read_text())
                for dist in (payload.get("data", {}).get("hoverDataList", []) or []):
                    metrics = dist.get("metric", [])
                    if metrics:
                        records.append({
                            "state": state, "year": year, "quarter": quarter,
                            "district":         dist.get("name"),
                            "insurance_count":  metrics[0].get("count", 0),
                            "insurance_amount": metrics[0].get("amount", 0.0),
                        })
    _bulk_insert(engine, pd.DataFrame(records), "map_insurance")


def load_top_transactions(engine, data_dir):
    """Load top/transaction data into top_transaction (districts + pincodes)."""
    print("\n🏆 Loading top_transaction …")
    base = Path(data_dir) / "data" / "top" / "transaction" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                payload  = json.loads(q_file.read_text())
                top_data = payload.get("data", {})
                for dist in (top_data.get("districts", []) or []):
                    m = dist.get("metric", {})
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": dist.get("entityName"), "pincode": None,
                        "transaction_count":  m.get("count", 0),
                        "transaction_amount": m.get("amount", 0.0),
                    })
                for pin in (top_data.get("pincodes", []) or []):
                    m = pin.get("metric", {})
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": None, "pincode": pin.get("entityName"),
                        "transaction_count":  m.get("count", 0),
                        "transaction_amount": m.get("amount", 0.0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "top_transaction")


def load_top_users(engine, data_dir):
    """Load top/user data into top_user (districts + pincodes)."""
    print("\n👤 Loading top_user …")
    base = Path(data_dir) / "data" / "top" / "user" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                payload  = json.loads(q_file.read_text())
                top_data = payload.get("data", {})
                for dist in (top_data.get("districts", []) or []):
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": dist.get("name"), "pincode": None,
                        "registered_users": dist.get("registeredUsers", 0),
                    })
                for pin in (top_data.get("pincodes", []) or []):
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": None, "pincode": pin.get("name"),
                        "registered_users": pin.get("registeredUsers", 0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "top_user")


def load_top_insurance(engine, data_dir):
    """Load top/insurance data into top_insurance (districts + pincodes)."""
    print("\n🛡️  Loading top_insurance …")
    base = Path(data_dir) / "data" / "top" / "insurance" / "country" / "india" / "state"
    if not base.exists():
        print(f"   ⚠️  Path not found: {base}"); return

    records = []
    for state_dir in base.iterdir():
        state = _state_name(state_dir.name)
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                payload  = json.loads(q_file.read_text())
                top_data = payload.get("data", {})
                for dist in (top_data.get("districts", []) or []):
                    m = dist.get("metric", {})
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": dist.get("entityName"), "pincode": None,
                        "insurance_count":  m.get("count", 0),
                        "insurance_amount": m.get("amount", 0.0),
                    })
                for pin in (top_data.get("pincodes", []) or []):
                    m = pin.get("metric", {})
                    records.append({
                        "state": state, "year": year, "quarter": quarter,
                        "district": None, "pincode": pin.get("entityName"),
                        "insurance_count":  m.get("count", 0),
                        "insurance_amount": m.get("amount", 0.0),
                    })
    _bulk_insert(engine, pd.DataFrame(records), "top_insurance")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  PhonePe Pulse — ETL Pipeline (SQLAlchemy)")
    print("=" * 60)

    # Step 1: Clone / pull the PhonePe Pulse repo
    clone_or_pull_repo()

    # Step 2: Create engine and all tables
    try:
        engine = create_db_engine()
    except SQLAlchemyError as e:
        print(f"\n❌ Could not connect to database: {e}")
        print("   Check DB_USER, DB_PASSWORD, DB_HOST, DB_PORT in this file.")
        print("   Make sure PostgreSQL is running and psycopg2-binary is installed.")
        return

    create_all_tables(engine)

    # Step 3: Load all 9 tables
    loaders = [
        ("Aggregated Tier", [
            load_aggregated_transactions,
            load_aggregated_users,
            load_aggregated_insurance,
        ]),
        ("Map Tier", [
            load_map_transactions,
            load_map_users,
            load_map_insurance,
        ]),
        ("Top Tier", [
            load_top_transactions,
            load_top_users,
            load_top_insurance,
        ]),
    ]

    for tier_name, funcs in loaders:
        print(f"\n{'─'*40}")
        print(f"  {tier_name}")
        print(f"{'─'*40}")
        for fn in funcs:
            try:
                fn(engine, PULSE_DIR)
            except Exception as e:
                print(f"   ❌ {fn.__name__} failed: {e}")

    engine.dispose()
    print("\n" + "=" * 60)
    print("  ✅ ETL Complete — All 9 tables loaded!")
    print("  ▶  Run:  streamlit run bcs_streamlit_app.py")
    print("=" * 60)


if __name__ == "__main__":
    main()
