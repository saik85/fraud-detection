"""Fraud-scoring logic as a pure Spark transform so it can be unit-tested
and reused in both batch and streaming.

Rules (illustrative): score points for high amount, high-risk country,
new device, and risky channel; flag when score crosses a threshold.
"""
from pyspark.sql import DataFrame
from pyspark.sql import functions as F

HIGH_RISK_COUNTRIES = ["NG", "RU"]
RISKY_CHANNELS = ["WIRE"]
FLAG_THRESHOLD = 60


def score(df: DataFrame) -> DataFrame:
    risk = (
        F.when(F.col("amount") >= 4000, 45)
        .when(F.col("amount") >= 1000, 20)
        .otherwise(0)
        + F.when(F.col("country").isin(HIGH_RISK_COUNTRIES), 35).otherwise(0)
        + F.when(F.col("device_new"), 20).otherwise(0)
        + F.when(F.col("channel").isin(RISKY_CHANNELS), 15).otherwise(0)
    )
    return (
        df.withColumn("risk_score", risk)
        .withColumn(
            "decision",
            F.when(F.col("risk_score") >= FLAG_THRESHOLD, F.lit("BLOCK"))
            .when(F.col("risk_score") >= 40, F.lit("REVIEW"))
            .otherwise(F.lit("ALLOW")),
        )
    )
