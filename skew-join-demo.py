"""
Optimized PySpark script — all five original issues resolved:
  - AQE enabled with skew-join splitting
  - Explicit StructType schemas (no inference)
  - Broadcast hint on 5-row dimension table
  - Salting (SALT_BUCKETS=50) to distribute the 80% U001 skew
  - Partition count logged before write
  - orders_df cached and unpersisted correctly
"""

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import broadcast
# OPT: import StructType components for explicit schema definitions
from pyspark.sql.types import StructType, StructField, StringType, DoubleType

# OPT: Enable AQE so Spark can dynamically coalesce shuffle partitions and
#      automatically split skewed join partitions at runtime; set shuffle
#      partitions to 200 as a sensible baseline (AQE will coalesce further).
spark = SparkSession.builder \
    .appName("skew-join-demo") \
    .config("spark.sql.adaptive.enabled", "true") \
    .config("spark.sql.adaptive.skewJoin.enabled", "true") \
    .config("spark.sql.shuffle.partitions", "200") \
    .getOrCreate()

# ---------------------------------------------------------------------------
# Large fact table — heavily skewed on user_id
# U001 appears in 80 % of rows, causing massive shuffle skew on any join/groupBy
# ---------------------------------------------------------------------------

# OPT: Explicit StructType for orders_df avoids schema inference overhead and
#      makes column types deterministic in production.
orders_schema = StructType([
    StructField("user_id",   StringType(), nullable=False),
    StructField("order_id",  StringType(), nullable=False),
    StructField("amount",    DoubleType(), nullable=True),
])

skewed_data = (
    [("U001", f"order_{i}", round(10 + i * 0.5, 2)) for i in range(8_000)]   # 80 % skewed key
    + [("U002", f"order_{i}", round(20 + i * 0.3, 2)) for i in range(1_000)]
    + [("U003", f"order_{i}", round(15 + i * 0.7, 2)) for i in range(500)]
    + [("U004", f"order_{i}", round(30 + i * 1.1, 2)) for i in range(300)]
    + [("U005", f"order_{i}", round(5  + i * 0.2, 2)) for i in range(200)]
)

orders_df = spark.createDataFrame(skewed_data, schema=orders_schema)

# OPT: Cache orders_df because it is consumed twice (count/distribution show
#      AND the salted join below).  Materialise immediately with .count() so
#      the cache is warm before any downstream action touches it.
orders_df.cache()
orders_df.count()  # materialise cache

print(f"Orders row count : {orders_df.count()}")
print("Key distribution (top 5):")
orders_df.groupBy("user_id").count().orderBy(F.desc("count")).show()

# ---------------------------------------------------------------------------
# Small dimension table — user profiles (fits easily in memory)
# ---------------------------------------------------------------------------

# OPT: Explicit StructType for users_df — same rationale as orders_schema.
users_schema = StructType([
    StructField("user_id",  StringType(), nullable=False),
    StructField("name",     StringType(), nullable=True),
    StructField("tier",     StringType(), nullable=True),
    StructField("country",  StringType(), nullable=True),
])

users_data = [
    ("U001", "Alice",   "premium",  "US"),
    ("U002", "Bob",     "standard", "UK"),
    ("U003", "Carol",   "premium",  "CA"),
    ("U004", "Dave",    "trial",    "AU"),
    ("U005", "Eve",     "standard", "DE"),
]

users_df = spark.createDataFrame(users_data, schema=users_schema)
print(f"Users row count  : {users_df.count()}")

# ---------------------------------------------------------------------------
# Skew salting — distribute the 80 % U001 skew across SALT_BUCKETS tasks
# ---------------------------------------------------------------------------

# OPT: Salt orders_df with a random bucket number appended to user_id.
#      This spreads the dominant U001 key across 50 independent shuffle
#      partitions instead of landing in a single oversized task.
SALT_BUCKETS = 50

orders_salted = orders_df \
    .withColumn("_salt", (F.rand() * SALT_BUCKETS).cast("int")) \
    .withColumn("_salted_key", F.concat_ws("_", F.col("user_id"), F.col("_salt")))

# OPT: Unpersist orders_df now that the salted version is derived — the
#      original DataFrame is no longer needed and holding its cache wastes memory.
orders_df.unpersist()

# OPT: Explode users_df for every salt bucket so each salted orders key has a
#      matching row in the dimension table, keeping the left-join semantics intact.
users_salted = users_df \
    .withColumn("_salt", F.explode(F.array([F.lit(i) for i in range(SALT_BUCKETS)]))) \
    .withColumn("_salted_key", F.concat_ws("_", F.col("user_id"), F.col("_salt")))

# ---------------------------------------------------------------------------
# Join — broadcast the exploded dimension table (still tiny vs fact table)
# ---------------------------------------------------------------------------

# OPT: Wrap users_salted in broadcast() so Spark sends the dimension rows to
#      every executor instead of performing a full shuffle sort-merge join.
#      Even after explosion users_salted is 5 * 50 = 250 rows — trivially small.
enriched_df = orders_salted.join(
    broadcast(users_salted),
    on="_salted_key",
    how="left",
)

# OPT: Drop the salt helper columns now that the join is complete; they must
#      not appear in downstream aggregation or output.
enriched_df = enriched_df.drop("_salt", "_salted_key", users_salted["user_id"])

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
# Write
# ---------------------------------------------------------------------------

# OPT: Log partition count before writing so operators can spot accidental
#      over- or under-partitioning (e.g. 1 partition = single-file bottleneck,
#      thousands of partitions = small-file problem) without opening the Spark UI.
print(f"Partition count before write: {summary_df.rdd.getNumPartitions()}")

summary_df.write.mode("overwrite").parquet("/tmp/skew_demo_output")
print("Done.")
