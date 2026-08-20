from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, sum as _sum, avg, when

spark = SparkSession.builder.appName("SmartInsuranceDataPlatform").getOrCreate()
root = "../data/raw"

customers = spark.read.option("header", True).option("inferSchema", True).csv(f"{root}/customers.csv")
policies = spark.read.option("header", True).option("inferSchema", True).csv(f"{root}/policies.csv")
claims = spark.read.option("header", True).option("inferSchema", True).csv(f"{root}/claims.csv")
payments = spark.read.option("header", True).option("inferSchema", True).csv(f"{root}/payments.csv")

print("Customers:", customers.count())
print("Policies:", policies.count())
print("Claims:", claims.count())
print("Payments:", payments.count())

customer_360 = (
    customers.alias("c")
    .join(policies.alias("p"), "customer_id", "left")
    .join(claims.alias("cl"), "policy_id", "left")
    .join(payments.alias("pay"), "policy_id", "left")
    .groupBy("customer_id", "customer_name", "city", "state")
    .agg(
        count("p.policy_id").alias("policy_count"),
        _sum("p.premium_amount").alias("total_premium"),
        count("cl.claim_id").alias("claim_count"),
        _sum("cl.claim_amount").alias("total_claim_amount"),
        _sum("pay.payment_amount").alias("total_payment_amount")
    )
)

customer_360.show(20, truncate=False)
spark.stop()
