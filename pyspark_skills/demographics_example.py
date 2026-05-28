"""
Example: detect, encrypt, and remove demographics from a PySpark DataFrame.
Run with: spark-submit demographics_example.py
"""

from pyspark.sql import SparkSession
from pyspark_skills import DemographicsHandler


def main():
    spark = SparkSession.builder.appName("demographics-demo").getOrCreate()

    # ── Sample data with mixed PII and non-PII columns ──────────────────────
    data = [
        (1, "Alice",   "Smith",  "alice@example.com",  "555-1234", "Engineer",  95000),
        (2, "Bob",     "Jones",  "bob@example.com",    "555-5678", "Analyst",   72000),
        (3, "Carol",   "White",  "carol@example.com",  "555-9012", "Manager",  110000),
    ]
    columns = ["id", "first_name", "last_name", "email", "phone", "job_title", "salary"]
    df = spark.createDataFrame(data, columns)

    print("=== Original DataFrame ===")
    df.show(truncate=False)

    # ── 1. Detect demographics ───────────────────────────────────────────────
    handler_hash = DemographicsHandler(mode="hash")
    report = handler_hash.detect_demographics(df)
    print("Detected demographics:", report["detected"])
    print("Clean columns:        ", report["clean"])

    # ── 2. Encrypt first_name + last_name with SHA-256 (hash mode) ──────────
    encrypted_df = handler_hash.encrypt_columns(df, columns=["first_name", "last_name"])
    print("\n=== SHA-256 Encrypted first_name / last_name ===")
    encrypted_df.show(truncate=False)

    # ── 3. AES-GCM encryption on first_name + last_name (Spark 3.3+) ────────
    AES_KEY = "zakipoint-secret-key-32chars!!!!"   # exactly 32 chars
    handler_aes = DemographicsHandler(encryption_key=AES_KEY, mode="aes")
    aes_df = handler_aes.encrypt_columns(df, columns=["first_name", "last_name"])
    print("\n=== AES-GCM Encrypted first_name / last_name ===")
    aes_df.show(truncate=False)

    # ── 4. Encrypt ALL detected demographic columns at once ──────────────────
    all_encrypted_df = handler_hash.encrypt_columns(df, encrypt_all_detected=True)
    print("\n=== All demographic columns hashed ===")
    all_encrypted_df.show(truncate=False)

    # ── 5. Remove all demographic columns ────────────────────────────────────
    clean_df = handler_hash.remove_demographics(df)
    print("\n=== DataFrame after removing all demographic columns ===")
    clean_df.show(truncate=False)

    # ── 6. Redact instead of remove ──────────────────────────────────────────
    redacted_df = handler_hash.redact_demographics(df)
    print("\n=== DataFrame with demographic columns redacted ===")
    redacted_df.show(truncate=False)

    spark.stop()


if __name__ == "__main__":
    main()
