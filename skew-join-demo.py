"""
Demo PySpark script — performance-optimised version.

Fixes applied:
  - AQE + skew-join enabled in SparkSession config
  - Explicit StructType schemas (no inference)
  - broadcast() hint on small users_df (5 rows)
  - Salting (SALT_BUCKETS = 50) to distribute the U001-skewed join key
  - Partition count logged before write
  - .coalesce() before write to avoid tiny output files
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# ---------------------------------------------------------------------------
# SparkSession — AQE + skew-join enabled
# ---------------------------------------------------------------------------

spark = SparkSession.builder \
    .appName("skew-join-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "400") \
    .getOrCreate()

# ---------------------------------------------------------------------------
# Explicit schemas
# ---------------------------------------------------------------------------

orders_schema = StructType([
    StructField("user_id",  StringType(), False),
    StructField("order_id", StringType(), False),
    StructField("amount",   DoubleType(), True),
])

users_schema = StructType([
    StructField("user_id", StringType(), False),
    StructField("name",    StringType(), True),
    StructField("tier",    StringType(), True),
    StructField("country", StringType(), True),
])

# ---------------------------------------------------------------------------
# Large fact table — heavily skewed on user_id
# ---------------------------------------------------------------------------

skewed_data = (
    [("U001", f"order_{i}", round(10 + i * 0.5, 2)) for i in range(8_000)]   # 80 % skewed key
    + [("U002", f"order_{i}", round(20 + i * 0.3, 2)) for i in range(1_000)]
    + [("U003", f"order_{i}", round(15 + i * 0.7, 2)) for i in range(500)]
    + [("U004", f"order_{i}", round(30 + i * 1.1, 2)) for i in range(300)]
    + [("U005", f"order_{i}", round(5  + i * 0.2, 2)) for i in range(200)]
)

orders_df = spark.createDataFrame(skewed_data, schema=orders_schema)

print(f"Orders row count : {orders_df.count()}")
print("Key distribution (top 5):")
orders_df.groupBy("user_id").count().orderBy(F.desc("count")).show()

# ---------------------------------------------------------------------------
# Small dimension table — user profiles (5 rows; broadcast-safe)
# ---------------------------------------------------------------------------

users_data = [
    ("U001", "Alice",   "premium", "US"),
    ("U002", "Bob",     "standard","UK"),
    ("U003", "Carol",   "premium", "CA"),
    ("U004", "Dave",    "trial",   "AU"),
    ("U005", "Eve",     "standard","DE"),
]

users_df = spark.createDataFrame(users_data, schema=users_schema)
print(f"Users row count  : {users_df.count()}")

# ---------------------------------------------------------------------------
# Salting — distribute the skewed U001 key across SALT_BUCKETS partitions
# ---------------------------------------------------------------------------

SALT_BUCKETS = 50

# Add random salt to the fact table
orders_salted = (
    orders_df
    .withColumn("salt", (F.rand() * SALT_BUCKETS).cast("int"))
    .withColumn("salted_key", F.concat(F.col("user_id"), F.lit("_"), F.col("salt")))
)

# Explode the small dimension table to match every salt bucket
users_exploded = (
    users_df
    .withColumn("salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)])))
    .withColumn("salted_key", F.concat(F.col("user_id"), F.lit("_"), F.col("salt")))
    .drop("user_id", "salt")          # avoid column name collision after join
)

# ---------------------------------------------------------------------------
# Join — broadcast the (still tiny) exploded dimension; drop salt columns
# ---------------------------------------------------------------------------

enriched_df = (
    orders_salted
    .join(F.broadcast(users_exploded), on="salted_key", how="left")
    .drop("salt", "salted_key")
)

# ---------------------------------------------------------------------------
# Aggregation
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
# Write — verify partition count; coalesce to avoid tiny files
# ---------------------------------------------------------------------------

summary_coalesced = summary_df.coalesce(1)
print(f"Partition count before write: {summary_coalesced.rdd.getNumPartitions()}")
summary_coalesced.write.mode("overwrite").parquet("/tmp/skew_demo_output")
print("Done.")
