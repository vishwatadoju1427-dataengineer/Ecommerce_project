from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import pandas as pd
import json


def create_spark_session(app_name="EcommercePlatform"):
    """Create and configure Spark session with optimized settings"""

    spark = (
        SparkSession.builder
        .appName(app_name)
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.databricks.delta.optimizeWrite.enabled", "true")
        .config("spark.databricks.delta.autoCompact.enabled", "true")
        .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
        .getOrCreate()
    )

    # Set log level
    spark.sparkContext.setLogLevel("WARN")

    return spark


# Explanation:
# Creates an optimized Spark session with Delta Lake configurations.
