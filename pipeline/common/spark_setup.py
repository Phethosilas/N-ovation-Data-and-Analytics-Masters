"""Spark session management"""

from pyspark.sql import SparkSession
from typing import Optional

_spark: Optional[SparkSession] = None


def get_spark_session(config: dict) -> SparkSession:
    """Get or create Spark session (hardened for container constraints)"""
    global _spark

    if _spark is not None:
        return _spark

    spark_config = config.get('spark', {})
    spark_conf = spark_config.get('config', {})

    builder = SparkSession.builder \
        .master(spark_config.get('master', 'local[2]')) \
        .appName(spark_config.get('app_name', 'nedbank-de-pipeline'))

    # Apply config from YAML
    for key, value in spark_conf.items():
        builder = builder.config(key, value)

    # ✅ CRITICAL FIXES FOR THIS CHALLENGE ENVIRONMENT
    builder = builder \
        .config("spark.sql.warehouse.dir", "/tmp/spark-warehouse") \
        .config("spark.local.dir", "/tmp/spark-temp") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.ui.enabled", "false") \
        .config("spark.sql.execution.arrow.pyspark.enabled", "false")

    _spark = builder.getOrCreate()

    # Reduce noise
    _spark.sparkContext.setLogLevel("WARN")

    return _spark


def stop_spark_session() -> None:
    """Stop Spark session"""
    global _spark
    if _spark is not None:
        _spark.stop()
        _spark = None