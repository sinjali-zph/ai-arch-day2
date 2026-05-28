# -*- coding: utf-8 -*-
"""
Created on Sat Jan 11 19:38:27 2020

@author: sparkbyexamples.com
"""

import pyspark
from pyspark.sql import SparkSession
from pyspark_skills.demographics_handler import DemographicsHandler


spark = SparkSession.builder.appName("SparkByExamples.com").getOrCreate()

states = {"NY": "New York", "CA": "California", "FL": "Florida"}
broadcastStates = spark.sparkContext.broadcast(states)

data = [
    ("James", "Smith", "USA", "CA"),
    ("Michael", "Rose", "USA", "NY"),
    ("Robert", "Williams", "USA", "CA"),
    ("Maria", "Jones", "USA", "FL"),
]

columns = ["firstname", "lastname", "country", "state"]
df = spark.createDataFrame(data=data, schema=columns)
df.printSchema()
df.show(truncate=False)


def state_convert(code):
    return broadcastStates.value[code]


result = df.rdd.map(lambda x: (x[0], x[1], x[2], state_convert(x[3]))).toDF(columns)
result.show(truncate=False)

# Broadcast variable on filter

filteDf = df.where((df["state"].isin(broadcastStates.value)))

# --- Demographics encryption ---
handler = DemographicsHandler(mode="hash")

report = handler.detect_demographics(result)
print("Detected PII columns:", report["detected"])
print("Clean columns:       ", report["clean"])

encrypted_result = handler.encrypt_columns(result, encrypt_all_detected=True)
print("\nEncrypted result (SHA-256):")
encrypted_result.show(truncate=False)
