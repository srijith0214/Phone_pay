"""
PhonePe Pulse Data Extraction & Loading Script
Clones the PhonePe Pulse GitHub repo and loads all JSON data into MySQL.
"""

import os
import json
import mysql.connector
from pathlib import Path
import subprocess
import sys

# ─────────────────────────────────────────────────────────────────
# CONFIG  – update these before running
# ─────────────────────────────────────────────────────────────────
DB_CONFIG = {
    "host": "localhost",
    "user": "root",
    "password": "",          # <-- your MySQL password
    "database": "phonepe_pulse",
}

GITHUB_REPO = "https://github.com/PhonePe/pulse.git"
DATA_DIR    = "./pulse"

# ─────────────────────────────────────────────────────────────────
# HELPERS
# ─────────────────────────────────────────────────────────────────
def clone_repo():
    if not os.path.exists(DATA_DIR):
        print("Cloning PhonePe Pulse repository …")
        subprocess.run(["git", "clone", GITHUB_REPO, DATA_DIR], check=True)
    else:
        print("Repository already exists. Pulling latest changes …")
        subprocess.run(["git", "-C", DATA_DIR, "pull"], check=True)


def get_conn():
    return mysql.connector.connect(**DB_CONFIG)


def ensure_db(conn):
    cur = conn.cursor()
    cur.execute(f"CREATE DATABASE IF NOT EXISTS {DB_CONFIG['database']}")
    cur.execute(f"USE {DB_CONFIG['database']}")
    conn.commit()
    cur.close()


# ─────────────────────────────────────────────────────────────────
# TABLE CREATION
# ─────────────────────────────────────────────────────────────────
DDL = """
CREATE TABLE IF NOT EXISTS Aggregated_transaction (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100), year SMALLINT, quarter TINYINT,
    transaction_type    VARCHAR(100),
    transaction_count   BIGINT, transaction_amount DECIMAL(20,2)
);
CREATE TABLE IF NOT EXISTS Aggregated_user (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100), year SMALLINT, quarter TINYINT,
    brand               VARCHAR(100), user_count BIGINT,
    registered_users    BIGINT, app_opens BIGINT
);
CREATE TABLE IF NOT EXISTS Aggregated_insurance (
    id                  BIGINT AUTO_INCREMENT PRIMARY KEY,
    state               VARCHAR(100), year SMALLINT, quarter TINYINT,
    insurance_count     BIGINT, insurance_amount DECIMAL(20,2)
);
CREATE TABLE IF NOT EXISTS Map_transaction (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), year SMALLINT, quarter TINYINT,
    transaction_count BIGINT, transaction_amount DECIMAL(20,2)
);
CREATE TABLE IF NOT EXISTS Map_user (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), year SMALLINT, quarter TINYINT,
    registered_users BIGINT, app_opens BIGINT
);
CREATE TABLE IF NOT EXISTS Map_insurance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), year SMALLINT, quarter TINYINT,
    insurance_count BIGINT, insurance_amount DECIMAL(20,2)
);
CREATE TABLE IF NOT EXISTS Top_transaction (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), pincode VARCHAR(20),
    year SMALLINT, quarter TINYINT,
    transaction_count BIGINT, transaction_amount DECIMAL(20,2)
);
CREATE TABLE IF NOT EXISTS Top_user (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), pincode VARCHAR(20),
    year SMALLINT, quarter TINYINT, registered_users BIGINT
);
CREATE TABLE IF NOT EXISTS Top_insurance (
    id BIGINT AUTO_INCREMENT PRIMARY KEY,
    state VARCHAR(100), district VARCHAR(200), pincode VARCHAR(20),
    year SMALLINT, quarter TINYINT,
    insurance_count BIGINT, insurance_amount DECIMAL(20,2)
);
"""

def create_tables(conn):
    cur = conn.cursor()
    for stmt in DDL.strip().split(";"):
        if stmt.strip():
            cur.execute(stmt)
    conn.commit()
    cur.close()
    print("✅ Tables created/verified.")


# ─────────────────────────────────────────────────────────────────
# LOADERS
# ─────────────────────────────────────────────────────────────────
def load_aggregated_transactions(conn, data_dir):
    path = Path(data_dir) / "data" / "aggregated" / "transaction" / "country" / "india" / "state"
    cur  = conn.cursor()
    rows = 0
    if not path.exists():
        print(f"⚠️  Path not found: {path}")
        return

    for state_dir in path.iterdir():
        state = state_dir.name.replace("-", " ").title()
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                data    = json.loads(q_file.read_text())
                for txn in data.get("data", {}).get("transactionData", []):
                    cur.execute(
                        "INSERT INTO Aggregated_transaction "
                        "(state, year, quarter, transaction_type, transaction_count, transaction_amount) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (state, year, quarter,
                         txn["name"],
                         txn["paymentInstruments"][0]["count"],
                         txn["paymentInstruments"][0]["amount"])
                    )
                    rows += 1
    conn.commit()
    cur.close()
    print(f"✅ Aggregated_transaction: {rows} rows inserted.")


def load_aggregated_users(conn, data_dir):
    path = Path(data_dir) / "data" / "aggregated" / "user" / "country" / "india" / "state"
    cur  = conn.cursor()
    rows = 0
    if not path.exists():
        print(f"⚠️  Path not found: {path}")
        return

    for state_dir in path.iterdir():
        state = state_dir.name.replace("-", " ").title()
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter  = int(q_file.stem)
                data     = json.loads(q_file.read_text())
                agg_data = data.get("data", {})
                reg_users = agg_data.get("aggregated", {}).get("registeredUsers", 0)
                app_opens = agg_data.get("aggregated", {}).get("appOpens", 0)
                for brand_data in agg_data.get("usersByDevice", []) or []:
                    cur.execute(
                        "INSERT INTO Aggregated_user "
                        "(state, year, quarter, brand, user_count, registered_users, app_opens) "
                        "VALUES (%s,%s,%s,%s,%s,%s,%s)",
                        (state, year, quarter,
                         brand_data.get("brand"),
                         brand_data.get("count", 0),
                         reg_users, app_opens)
                    )
                    rows += 1
    conn.commit()
    cur.close()
    print(f"✅ Aggregated_user: {rows} rows inserted.")


def load_aggregated_insurance(conn, data_dir):
    path = Path(data_dir) / "data" / "aggregated" / "insurance" / "country" / "india" / "state"
    cur  = conn.cursor()
    rows = 0
    if not path.exists():
        print(f"⚠️  Path not found: {path}")
        return

    for state_dir in path.iterdir():
        state = state_dir.name.replace("-", " ").title()
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                data    = json.loads(q_file.read_text())
                txn_data = (data.get("data", {})
                            .get("transactionData", []) or [])
                if txn_data:
                    cur.execute(
                        "INSERT INTO Aggregated_insurance "
                        "(state, year, quarter, insurance_count, insurance_amount) "
                        "VALUES (%s,%s,%s,%s,%s)",
                        (state, year, quarter,
                         txn_data[0]["paymentInstruments"][0]["count"],
                         txn_data[0]["paymentInstruments"][0]["amount"])
                    )
                    rows += 1
    conn.commit()
    cur.close()
    print(f"✅ Aggregated_insurance: {rows} rows inserted.")


def load_map_transactions(conn, data_dir):
    path = Path(data_dir) / "data" / "map" / "transaction" / "hover" / "country" / "india" / "state"
    cur  = conn.cursor()
    rows = 0
    if not path.exists():
        print(f"⚠️  Path not found: {path}")
        return

    for state_dir in path.iterdir():
        state = state_dir.name.replace("-", " ").title()
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                data    = json.loads(q_file.read_text())
                for dist in (data.get("data", {}).get("hoverDataList", []) or []):
                    pmt = dist.get("metric", [])
                    if pmt:
                        cur.execute(
                            "INSERT INTO Map_transaction "
                            "(state, district, year, quarter, transaction_count, transaction_amount) "
                            "VALUES (%s,%s,%s,%s,%s,%s)",
                            (state,
                             dist.get("name"),
                             year, quarter,
                             pmt[0].get("count", 0),
                             pmt[0].get("amount", 0))
                        )
                        rows += 1
    conn.commit()
    cur.close()
    print(f"✅ Map_transaction: {rows} rows inserted.")


def load_top_transactions(conn, data_dir):
    path = Path(data_dir) / "data" / "top" / "transaction" / "country" / "india" / "state"
    cur  = conn.cursor()
    rows = 0
    if not path.exists():
        print(f"⚠️  Path not found: {path}")
        return

    for state_dir in path.iterdir():
        state = state_dir.name.replace("-", " ").title()
        for year_dir in state_dir.iterdir():
            year = int(year_dir.name)
            for q_file in year_dir.glob("*.json"):
                quarter = int(q_file.stem)
                data    = json.loads(q_file.read_text())
                top_data = data.get("data", {})

                for dist in (top_data.get("districts", []) or []):
                    cur.execute(
                        "INSERT INTO Top_transaction "
                        "(state, district, year, quarter, transaction_count, transaction_amount) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (state, dist.get("entityName"), year, quarter,
                         dist.get("metric", {}).get("count", 0),
                         dist.get("metric", {}).get("amount", 0))
                    )
                    rows += 1

                for pin in (top_data.get("pincodes", []) or []):
                    cur.execute(
                        "INSERT INTO Top_transaction "
                        "(state, pincode, year, quarter, transaction_count, transaction_amount) "
                        "VALUES (%s,%s,%s,%s,%s,%s)",
                        (state, pin.get("entityName"), year, quarter,
                         pin.get("metric", {}).get("count", 0),
                         pin.get("metric", {}).get("amount", 0))
                    )
                    rows += 1

    conn.commit()
    cur.close()
    print(f"✅ Top_transaction: {rows} rows inserted.")


# ─────────────────────────────────────────────────────────────────
# MAIN
# ─────────────────────────────────────────────────────────────────
def main():
    print("=" * 60)
    print("  PhonePe Pulse – Data Extraction & Loading")
    print("=" * 60)

    # Step 1: Clone / update repo
    clone_repo()

    # Step 2: Connect to DB
    conn = get_conn()
    ensure_db(conn)
    create_tables(conn)

    # Step 3: Load all tables
    load_aggregated_transactions(conn, DATA_DIR)
    load_aggregated_users(conn, DATA_DIR)
    load_aggregated_insurance(conn, DATA_DIR)
    load_map_transactions(conn, DATA_DIR)
    load_top_transactions(conn, DATA_DIR)

    conn.close()
    print("\n✅ All data loaded successfully!")
    print("   Run:  streamlit run phonepe_app.py")


if __name__ == "__main__":
    main()
