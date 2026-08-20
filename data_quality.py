import pandas as pd
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"

files = ["customers.csv","policies.csv","claims.csv","payments.csv"]
for file in files:
    df = pd.read_csv(RAW/file)
    print(f"\n{file}")
    print("Rows:", len(df))
    print("Duplicate rows:", df.duplicated().sum())
    print("Null values:", int(df.isna().sum().sum()))
