"""Real-time fraud scoring with Spark Structured Streaming.

Reads the JSON event stream micro-batch by micro-batch, scores each transaction,
routes ALLOW / REVIEW / BLOCK, and writes flagged (REVIEW/BLOCK) transactions to
a sink — the same pattern used on Kinesis/Kafka in production, here on a file
source so it runs anywhere with just PySpark.
"""
import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType, BooleanType,
)

from scoring import score

STREAM_DIR = os.path.join("data", "stream")
FLAGGED = os.path.join("data", "flagged")
CHECKPOINT = os.path.join("data", "_checkpoint")

SCHEMA = StructType([
    StructField("txn_id", StringType()),
    StructField("event_time", StringType()),
    StructField("account_id", StringType()),
    StructField("channel", StringType()),
    StructField("amount", DoubleType()),
    StructField("country", StringType()),
    StructField("device_new", BooleanType()),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder.appName("realtime-fraud")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.ui.showConsoleProgress", "false")
        .getOrCreate()
    )


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("ERROR")

    stream = (
        spark.readStream.schema(SCHEMA)
        .option("maxFilesPerTrigger", 1)   # one micro-batch per file
        .json(STREAM_DIR)
    )

    scored = score(stream)
    flagged = scored.filter(F.col("decision") != "ALLOW")

    def handle_batch(df, batch_id):
        rows = df.count()
        by_decision = {r["decision"]: r["cnt"] for r in
                       df.groupBy("decision").agg(F.count("*").alias("cnt")).collect()}
        print(f"  [micro-batch {batch_id}] flagged={rows}  "
              f"BLOCK={by_decision.get('BLOCK',0)}  REVIEW={by_decision.get('REVIEW',0)}")
        (df.write.mode("append").parquet(FLAGGED))

    query = (
        flagged.writeStream.foreachBatch(handle_batch)
        .option("checkpointLocation", CHECKPOINT)
        .trigger(processingTime="1 second")
        .start()
    )

    print("Streaming fraud scorer started — processing event files...")
    query.awaitTermination(timeout=25)   # run ~25s for the demo then stop
    query.stop()

    total = spark.read.parquet(FLAGGED)
    print(f"\n  ✓ total flagged transactions: {total.count():,}")
    print("\n  Sample of flagged (REVIEW / BLOCK):")
    (total.select("txn_id", "channel", "amount", "country", "device_new",
                  "risk_score", "decision")
          .orderBy(F.col("risk_score").desc()).show(8, truncate=False))
    spark.stop()


if __name__ == "__main__":
    main()
