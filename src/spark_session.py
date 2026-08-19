"""SparkSession construction shared by the processor and tests."""

from __future__ import annotations

from pyspark.sql import SparkSession


def create_local_spark(
    app_name: str,
    master: str = "local[2]",
) -> SparkSession:
    """Create the small, UTC Spark runtime used by this local project."""
    spark = (
        SparkSession.builder.appName(app_name)
        .master(master)
        .config("spark.sql.session.timeZone", "UTC")
        .config("spark.sql.caseSensitive", "true")
        .config("spark.ui.enabled", "false")
        .config("spark.sql.shuffle.partitions", "2")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("ERROR")
    return spark
