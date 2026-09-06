import os, sys
import pytest
from pyspark.sql import SparkSession
sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))
from scoring import score  # noqa: E402


@pytest.fixture(scope="session")
def spark():
    s = SparkSession.builder.appName("t").master("local[1]").getOrCreate()
    yield s; s.stop()


def _df(spark, rows):
    cols = ["txn_id", "amount", "country", "device_new", "channel"]
    return spark.createDataFrame(rows, cols)


def test_large_foreign_new_device_is_blocked(spark):
    r = score(_df(spark, [("T1", 5000.0, "NG", True, "WIRE")])).collect()[0]
    assert r["risk_score"] >= 60 and r["decision"] == "BLOCK"


def test_small_domestic_is_allowed(spark):
    r = score(_df(spark, [("T2", 20.0, "US", False, "CARD")])).collect()[0]
    assert r["decision"] == "ALLOW"


def test_midrange_is_review(spark):
    r = score(_df(spark, [("T3", 1500.0, "US", True, "CARD")])).collect()[0]
    assert r["decision"] in ("REVIEW", "BLOCK")
