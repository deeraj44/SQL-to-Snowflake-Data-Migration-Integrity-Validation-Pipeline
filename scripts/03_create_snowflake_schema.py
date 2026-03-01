import os
from dotenv import load_dotenv
import snowflake.connector

load_dotenv()

def connect():
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        password=os.environ["SNOWFLAKE_PASSWORD"],
        warehouse=os.environ["SNOWFLAKE_WAREHOUSE"],
        role=os.getenv("SNOWFLAKE_ROLE"),
    )

def main():
    db = os.environ["SNOWFLAKE_DATABASE"]
    schema = os.environ["SNOWFLAKE_SCHEMA"]

    ddl = f"""
    CREATE DATABASE IF NOT EXISTS {db};
    CREATE SCHEMA IF NOT EXISTS {db}.{schema};

    CREATE OR REPLACE TABLE {db}.{schema}.transactions (
      txn_id NUMBER(38,0),
      amount FLOAT,
      ingested_at TIMESTAMP_NTZ DEFAULT CURRENT_TIMESTAMP(),
      PRIMARY KEY (txn_id)
    );

    CREATE OR REPLACE TABLE {db}.{schema}.transaction_features (
      txn_id NUMBER(38,0),
      v1 FLOAT, v2 FLOAT, v3 FLOAT, v4 FLOAT, v5 FLOAT, v6 FLOAT, v7 FLOAT, v8 FLOAT,
      v9 FLOAT, v10 FLOAT, v11 FLOAT, v12 FLOAT, v13 FLOAT, v14 FLOAT, v15 FLOAT, v16 FLOAT,
      v17 FLOAT, v18 FLOAT, v19 FLOAT, v20 FLOAT, v21 FLOAT, v22 FLOAT, v23 FLOAT, v24 FLOAT,
      v25 FLOAT, v26 FLOAT, v27 FLOAT, v28 FLOAT,
      PRIMARY KEY (txn_id)
    );

    CREATE OR REPLACE TABLE {db}.{schema}.fraud_labels (
      txn_id NUMBER(38,0),
      is_fraud NUMBER(1,0),
      PRIMARY KEY (txn_id)
    );
    """

    con = connect()
    try:
        cur = con.cursor()
        for stmt in [s.strip() for s in ddl.split(";") if s.strip()]:
            cur.execute(stmt)
        print("Snowflake schema created.")
    finally:
        con.close()

if __name__ == "__main__":
    main()