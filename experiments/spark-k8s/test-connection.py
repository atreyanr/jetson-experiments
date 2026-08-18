#!/usr/bin/env python3
"""Smoke test: Spark Connect on k3s."""

from pyspark.sql import SparkSession
import pyspark.sql.functions as F

spark = (
    SparkSession.builder
    .remote("sc://localhost:15002")
    .getOrCreate()
)

# 1. Basic connectivity
print("=== spark.range ===")
spark.range(10).show()

# 2. DataFrame operations over Connect
print("=== DataFrame ops ===")
df = spark.createDataFrame(
    [(1, "gpu", 3.14), (2, "cpu", 2.71), (3, "gpu", 1.41)],
    schema="id INT, device STRING, value DOUBLE",
)
df.groupBy("device").agg(F.sum("value").alias("total")).show()

print("✓ Spark Connect on k3s is working")
spark.stop()
