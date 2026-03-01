import pandas as pd
from pathlib import Path

CSV_PATH = Path("data/creditcard_2023.csv")

def main():
    df = pd.read_csv(CSV_PATH)

    # Basic shape
    profile = {
        "rows": len(df),
        "columns": df.shape[1],
        "duplicate_id_count": int(df["id"].duplicated().sum()),
        "null_total": int(df.isna().sum().sum()),
        "fraud_count": int((df["Class"] == 1).sum()),
        "nonfraud_count": int((df["Class"] == 0).sum()),
        "fraud_rate": float((df["Class"] == 1).mean()),
        "amount_min": float(df["Amount"].min()),
        "amount_max": float(df["Amount"].max()),
        "amount_mean": float(df["Amount"].mean()),
    }

    out = pd.DataFrame([profile])
    out.to_csv("docs/source_profile.csv", index=False)
    print(out.to_string(index=False))

if __name__ == "__main__":
    main()