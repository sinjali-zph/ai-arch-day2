"""
Demo PySpark script with intentional performance issues:
  - Heavily skewed join key (user_id "U001" dominates)
  - Small dimension table joined without broadcast hint
  - Missing AQE config
  - No partition verification before write
  - Schema inference instead of explicit StructType
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F

spark = SparkSession.builder \
    .appName("skew-join-demo") \
    .getOrCreate()

# ---------------------------------------------------------------------------
# Large fact table — heavily skewed on user_id
# U001 appears in 80 % of rows, causing massive shuffle skew on any join/groupBy
# ---------------------------------------------------------------------------

skewed_data = (
    [("U001", f"order_{i}", round(10 + i * 0.5, 2)) for i in range(8_000)]   # 80 % skewed key
    + [("U002", f"order_{i}", round(20 + i * 0.3, 2)) for i in range(1_000)]
    + [("U003", f"order_{i}", round(15 + i * 0.7, 2)) for i in range(500)]
    + [("U004", f"order_{i}", round(30 + i * 1.1, 2)) for i in range(300)]
    + [("U005", f"order_{i}", round(5  + i * 0.2, 2)) for i in range(200)]
)

orders_df = spark.createDataFrame(skewed_data, ["user_id", "order_id", "amount"])

print(f"Orders row count : {orders_df.count()}")
print("Key distribution (top 5):")
orders_df.groupBy("user_id").count().orderBy(F.desc("count")).show()

# ---------------------------------------------------------------------------
# Small dimension table — user profiles (fits easily in memory)
# Should be broadcast-joined but hint is intentionally missing
# ---------------------------------------------------------------------------

users_data = [
    ("U001", "Alice",   "premium", "US"),
    ("U002", "Bob",     "standard","UK"),
    ("U003", "Carol",   "premium", "CA"),
    ("U004", "Dave",    "trial",   "AU"),
    ("U005", "Eve",     "standard","DE"),
]

users_df = spark.createDataFrame(users_data, ["user_id", "name", "tier", "country"])
print(f"Users row count  : {users_df.count()}")

# ---------------------------------------------------------------------------
# Join — no broadcast hint, will trigger a full shuffle join despite
# users_df being tiny (5 rows)
# ---------------------------------------------------------------------------

enriched_df = orders_df.join(users_df, on="user_id", how="left")

# ---------------------------------------------------------------------------
# Aggregation — skewed user_id causes one task to process 80 % of data
# ---------------------------------------------------------------------------

summary_df = enriched_df \
    .groupBy("user_id", "name", "tier", "country") \
    .agg(
        F.sum("amount").alias("total_spend"),
        F.count("order_id").alias("order_count"),
        F.avg("amount").alias("avg_order_value"),
    ) \
    .orderBy(F.desc("total_spend"))

summary_df.show(truncate=False)

# ---------------------------------------------------------------------------
# Write — no partition count check before writing
# ---------------------------------------------------------------------------

summary_df.write.mode("overwrite").parquet("/tmp/skew_demo_output")
print("Done.")
