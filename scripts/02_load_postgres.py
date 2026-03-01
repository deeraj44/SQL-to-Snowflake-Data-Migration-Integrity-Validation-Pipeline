import os
import pandas as pd
from sqlalchemy import create_engine
from pathlib import Path

CSV_PATH = Path("data/creditcard_2023.csv")
from sqlalchemy.engine import URL
from sqlalchemy import create_engine

url = URL.create(
    drivername="postgresql+psycopg2",
    username="postgres",
    password="A@qwerTy299",
    host="localhost",
    port=5432,
    database="migration"
)

engine = create_engine(url)

POSTGRES_URL = engine.url

def main():
    df = pd.read_csv(CSV_PATH)

    # Normalize column names
    # Kaggle file uses V1..V28; keep as lower-case v1..v28 in DB
    feature_cols = [f"V{i}" for i in range(1, 29)]
    rename_map = {c: c.lower() for c in feature_cols}

    df_features = df[["id"] + feature_cols].rename(columns={"id": "txn_id", **rename_map})
    df_txn = df[["id", "Amount"]].rename(columns={"id": "txn_id", "Amount": "amount"})
    df_labels = df[["id", "Class"]].rename(columns={"id": "txn_id", "Class": "is_fraud"})

    # Connect
    engine = create_engine(POSTGRES_URL)

    # Load order matters because of foreign keys
    # Use replace only if you want a fresh load each run; otherwise use append
    with engine.begin() as conn:
        df_txn.to_sql("transactions", conn, if_exists="append", index=False, method="multi", chunksize=50_000)
        df_features.to_sql("transaction_features", conn, if_exists="append", index=False, method="multi", chunksize=50_000)
        df_labels.to_sql("fraud_labels", conn, if_exists="append", index=False, method="multi", chunksize=50_000)

    print("Loaded into Postgres:")
    print(f"transactions: {len(df_txn)}")
    print(f"transaction_features: {len(df_features)}")
    print(f"fraud_labels: {len(df_labels)}")

if __name__ == "__main__":
    main()