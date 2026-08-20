"""
Smart Insurance Data Platform - Local ETL Pipeline
Run: python python/run_pipeline.py
Requires: pandas
"""

from pathlib import Path
import pandas as pd

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
BRONZE = ROOT / "data" / "bronze"
SILVER = ROOT / "data" / "silver"
GOLD = ROOT / "data" / "gold"

for p in [BRONZE, SILVER, GOLD]:
    p.mkdir(parents=True, exist_ok=True)

def load_raw():
    return {
        "customers": pd.read_csv(RAW / "customers.csv"),
        "policies": pd.read_csv(RAW / "policies.csv"),
        "claims": pd.read_csv(RAW / "claims.csv"),
        "payments": pd.read_csv(RAW / "payments.csv"),
    }

def bronze_ingestion(dfs):
    for name, df in dfs.items():
        out = BRONZE / f"{name}_bronze.csv"
        df.to_csv(out, index=False)
    print("Bronze layer created.")

def data_quality(dfs):
    print("\n=== DATA QUALITY ===")
    for name, df in dfs.items():
        print(f"{name}: rows={len(df)}, duplicate_rows={df.duplicated().sum()}, nulls={int(df.isna().sum().sum())}")
    checks = [
        ("customers_customer_id_unique", dfs["customers"]["customer_id"].is_unique),
        ("policies_customer_id_valid", dfs["policies"]["customer_id"].isin(dfs["customers"]["customer_id"]).all()),
        ("claims_policy_id_valid", dfs["claims"]["policy_id"].isin(dfs["policies"]["policy_id"]).all()),
        ("payments_policy_id_valid", dfs["payments"]["policy_id"].isin(dfs["policies"]["policy_id"]).all()),
        ("claims_amount_non_negative", (dfs["claims"]["claim_amount"] >= 0).all()),
        ("payments_amount_non_negative", (dfs["payments"]["payment_amount"] >= 0).all()),
    ]
    for check, ok in checks:
        print(f"{check}: {'PASS' if ok else 'FAIL'}")

def silver_transform(dfs):
    c = dfs["customers"].copy()
    p = dfs["policies"].copy()
    cl = dfs["claims"].copy()
    pay = dfs["payments"].copy()

    c["registration_date"] = pd.to_datetime(c["registration_date"])
    c["date_of_birth"] = pd.to_datetime(c["date_of_birth"])
    p["policy_start_date"] = pd.to_datetime(p["policy_start_date"])
    p["policy_end_date"] = pd.to_datetime(p["policy_end_date"])
    cl["claim_date"] = pd.to_datetime(cl["claim_date"])
    pay["payment_date"] = pd.to_datetime(pay["payment_date"])

    for df in [c,p,cl,pay]:
        for col in df.select_dtypes(include="object").columns:
            df[col] = df[col].astype(str).str.strip()

    c = c.drop_duplicates(subset=["customer_id"])
    p = p.drop_duplicates(subset=["policy_id"])
    cl = cl.drop_duplicates(subset=["claim_id"])
    pay = pay.drop_duplicates(subset=["payment_id"])

    for name, df in {"customers":c,"policies":p,"claims":cl,"payments":pay}.items():
        df.to_csv(SILVER / f"{name}_silver.csv", index=False)

    return c,p,cl,pay

def gold_transform(c,p,cl,pay):
    customer_policy = p.groupby("customer_id").agg(
        total_policies=("policy_id","count"),
        active_policies=("policy_status", lambda x: (x=="Active").sum()),
        total_premium=("premium_amount","sum"),
        avg_premium=("premium_amount","mean")
    ).reset_index()

    customer_claims = cl.groupby("customer_id").agg(
        total_claims=("claim_id","count"),
        total_claim_amount=("claim_amount","sum"),
        total_approved_amount=("approved_amount","sum")
    ).reset_index()

    customer_payments = pay.groupby("customer_id").agg(
        total_payments=("payment_id","count"),
        total_payment_amount=("payment_amount","sum"),
        paid_payments=("payment_status", lambda x: (x=="Paid").sum())
    ).reset_index()

    customer_360 = c.merge(customer_policy,on="customer_id",how="left") \
                    .merge(customer_claims,on="customer_id",how="left") \
                    .merge(customer_payments,on="customer_id",how="left")

    numeric_cols = customer_360.select_dtypes(include="number").columns
    customer_360[numeric_cols] = customer_360[numeric_cols].fillna(0)
    customer_360["claim_to_premium_ratio"] = (
        customer_360["total_claim_amount"] / customer_360["total_premium"].replace(0, pd.NA)
    ).fillna(0).round(4)

    policy_summary = p.merge(
        cl.groupby("policy_id").agg(
            claim_count=("claim_id","count"),
            total_claim_amount=("claim_amount","sum"),
            total_approved_amount=("approved_amount","sum")
        ).reset_index(), on="policy_id", how="left"
    )
    policy_summary[["claim_count","total_claim_amount","total_approved_amount"]] = \
        policy_summary[["claim_count","total_claim_amount","total_approved_amount"]].fillna(0)

    claims_by_type = cl.groupby("claim_type").agg(
        claim_count=("claim_id","count"),
        total_claim_amount=("claim_amount","sum"),
        total_approved_amount=("approved_amount","sum")
    ).reset_index()

    payment_summary = pay.groupby(["payment_method","payment_status"]).agg(
        payment_count=("payment_id","count"),
        total_amount=("payment_amount","sum")
    ).reset_index()

    customer_360.to_csv(GOLD / "customer_360.csv", index=False)
    policy_summary.to_csv(GOLD / "policy_summary.csv", index=False)
    claims_by_type.to_csv(GOLD / "claims_by_type.csv", index=False)
    payment_summary.to_csv(GOLD / "payment_summary.csv", index=False)

    print("Gold layer created.")

if __name__ == "__main__":
    dfs = load_raw()
    bronze_ingestion(dfs)
    data_quality(dfs)
    c,p,cl,pay = silver_transform(dfs)
    gold_transform(c,p,cl,pay)
    print("\nETL pipeline completed successfully.")
