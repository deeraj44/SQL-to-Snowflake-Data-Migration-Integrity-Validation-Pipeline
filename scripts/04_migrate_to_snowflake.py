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

def chunked_query(engine, query, chunksize=50_000):
    return pd.read_sql_query(query, engine, chunksize=chunksize)

def insert_df_sf(cur, table, df):
    # Convert datetime columns to strings for Snowflake
    for col in df.columns:
        if pd.api.types.is_datetime64_any_dtype(df[col]):
            df[col] = df[col].astype(str)

    cols = list(df.columns)
    placeholders = ", ".join(["%s"] * len(cols))
    collist = ", ".join(cols)
    sql = f"INSERT INTO {table} ({collist}) VALUES ({placeholders})"

    cur.executemany(sql, df.itertuples(index=False, name=None))

def main():
    db = os.environ["SNOWFLAKE_DATABASE"]
    schema = os.environ["SNOWFLAKE_SCHEMA"]

    tables = {
        "fraud_labels": "SELECT * FROM fraud_labels",
        "transactions": "SELECT txn_id, amount, ingested_at FROM transactions",
        "transaction_features": "SELECT * FROM transaction_features",
    }

    eng = pg_engine()
    con = sf_connect()

    try:
        cur = con.cursor()

        # Clear target tables to make script rerunnable
        for t in tables.keys():
            cur.execute(f"TRUNCATE TABLE {db}.{schema}.{t}")

        for t, q in tables.items():
            full_table = f"{db}.{schema}.{t}"
            total = 0
            for chunk in chunked_query(eng, q, chunksize=50_000):
                insert_df_sf(cur, full_table, chunk)
                total += len(chunk)
                print(f"{t}: migrated {total} rows")
            print(f"✅ Finished {t}: {total} rows")

        con.commit()
        print("✅ Migration complete.")
    finally:
        con.close()

if __name__ == "__main__":
    main()