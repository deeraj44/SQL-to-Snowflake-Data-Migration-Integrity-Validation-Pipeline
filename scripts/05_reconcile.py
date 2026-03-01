import os
import pandas as pd
from sqlalchemy import create_engine
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()
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

def pg_engine():
    return create_engine(POSTGRES_URL)

def sf_connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.getenv("SNOWFLAKE_ROLE"),
        database=os.environ["SNOWFLAKE_DATABASE"],
        schema=os.environ["SNOWFLAKE_SCHEMA"],
    )

def get_pg_counts(eng):
    queries = {
        "transactions_rows": "SELECT COUNT(*) c FROM transactions",
        "features_rows": "SELECT COUNT(*) c FROM transaction_features",
        "labels_rows": "SELECT COUNT(*) c FROM fraud_labels",
        "fraud_sum": "SELECT COALESCE(SUM(is_fraud),0) c FROM fraud_labels",
        "amount_min": "SELECT MIN(amount) c FROM transactions",
        "amount_max": "SELECT MAX(amount) c FROM transactions",
    }
    out = {}
    for k,q in queries.items():
        out[k] = pd.read_sql_query(q, eng)["c"].iloc[0]
    return out

def get_sf_counts(cur, db, schema):
    queries = {
        "transactions_rows": f"SELECT COUNT(*) c FROM {db}.{schema}.transactions",
        "features_rows": f"SELECT COUNT(*) c FROM {db}.{schema}.transaction_features",
        "labels_rows": f"SELECT COUNT(*) c FROM {db}.{schema}.fraud_labels",
        "fraud_sum": f"SELECT COALESCE(SUM(is_fraud),0) c FROM {db}.{schema}.fraud_labels",
        "amount_min": f"SELECT MIN(amount) c FROM {db}.{schema}.transactions",
        "amount_max": f"SELECT MAX(amount) c FROM {db}.{schema}.transactions",
    }
    out = {}
    for k,q in queries.items():
        cur.execute(q)
        out[k] = cur.fetchone()[0]
    return out

def main():
    db = os.environ["SNOWFLAKE_DATABASE"]
    schema = os.environ["SNOWFLAKE_SCHEMA"]

    eng = pg_engine()
    pg = get_pg_counts(eng)

    con = sf_connect()
    try:
        cur = con.cursor()
        sf = get_sf_counts(cur, db, schema)
    finally:
        con.close()

    rows = []
    for metric in pg.keys():
        rows.append({
            "metric": metric,
            "postgres_value": float(pg[metric]) if pg[metric] is not None else None,
            "snowflake_value": float(sf[metric]) if sf[metric] is not None else None,
            "match": pg[metric] == sf[metric]
        })

    report = pd.DataFrame(rows)
    report.to_csv("docs/recon_report.csv", index=False)
    print(report.to_string(index=False))

if __name__ == "__main__":
    main()