"""
Stage 3: Bronze Layer Ingestion Script
Reads raw Parquet files from Azure Blob (bronze container),
applies schema detection, adds lineage metadata, and writes to
a partitioned Delta table (or Parquet if Delta not available locally).

Run locally with PySpark or upload to Databricks Community Edition.
"""

import os
import sys
import logging
from datetime import datetime, timezone

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, MapType
)
from stage1_infrastructure.config.settings import config

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def get_spark() -> SparkSession:
    """Create Spark session — works locally and on Databricks Community."""
    builder = (
        SparkSession.builder
        .appName("RetailBronzeIngestion")
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension")
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")
        # Optimize for retail event workloads
        .config("spark.sql.shuffle.partitions", "200")
        .config("spark.sql.adaptive.enabled", "true")
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    )

    # Local mode only — Databricks provides its own cluster
    if not os.environ.get("DATABRICKS_RUNTIME_VERSION"):
        builder = builder.master("local[*]")

    return builder.getOrCreate()


def read_bronze_blobs(spark: SparkSession, storage_path: str) -> dict:
    """
    Read all raw Parquet files from bronze container, grouped by source topic.
    Returns dict of {topic_name: DataFrame}.
    """
    topics = {
        "pos":       "retail.pos.events",
        "inventory": "retail.inventory.events",
        "ecommerce": "retail.ecommerce.events",
    }
    dfs = {}
    for name, topic in topics.items():
        path = f"{storage_path}/{topic}"
        try:
            df = spark.read.parquet(path)
            count = df.count()
            log.info("Loaded %s: %d records from %s", name, count, path)
            dfs[name] = df
        except Exception as exc:
            log.warning("Could not read %s from %s: %s", name, path, exc)
    return dfs


def add_bronze_metadata(df, source_name: str):
    """Add lineage and ingestion metadata columns to bronze DataFrame."""
    return df.withColumn(
        "_bronze_loaded_at", F.lit(datetime.now(timezone.utc).isoformat())
    ).withColumn(
        "_source_name", F.lit(source_name)
    ).withColumn(
        "_record_hash", F.md5(F.to_json(F.struct(*df.columns)))
    ).withColumn(
        "event_date", F.to_date(F.col("timestamp"))
    ).withColumn(
        "event_hour", F.hour(F.to_timestamp(F.col("timestamp")))
    )


def validate_bronze_schema(df, source: str) -> tuple:
    """
    Basic schema validation — flag records missing required fields.
    Returns (valid_df, invalid_df).
    """
    required_by_source = {
        "pos":       ["event_id", "store_id", "transaction_id", "timestamp"],
        "inventory": ["event_id", "store_id", "sku", "timestamp", "quantity_on_hand"],
        "ecommerce": ["event_id", "session_id", "customer_id", "timestamp"],
    }
    required = required_by_source.get(source, [])
    null_check = F.lit(False)
    for col_name in required:
        if col_name in df.columns:
            null_check = null_check | F.col(col_name).isNull()

    valid_df   = df.filter(~null_check)
    invalid_df = df.filter(null_check).withColumn("_validation_error", F.lit("missing_required_field"))

    valid_count   = valid_df.count()
    invalid_count = invalid_df.count()
    log.info("[%s] validation → valid=%d invalid=%d", source, valid_count, invalid_count)
    return valid_df, invalid_df


def write_bronze_table(df, output_path: str, source: str, partition_cols: list[str]) -> None:
    """Write validated bronze data as partitioned Parquet (or Delta in Databricks)."""
    dest = f"{output_path}/bronze_{source}"
    (
        df.write
        .mode("append")
        .partitionBy(*partition_cols)
        .parquet(dest)
    )
    log.info("Written bronze_%s → %s (partitions: %s)", source, dest, partition_cols)


def write_quarantine(df, output_path: str, source: str) -> None:
    """Write invalid records to quarantine zone for investigation."""
    if df.isEmpty():
        return
    dest = f"{output_path}/quarantine/{source}/{datetime.now().strftime('%Y/%m/%d')}"
    df.write.mode("append").parquet(dest)
    log.info("Quarantined invalid records → %s", dest)


def run_bronze_pipeline(storage_path: str = None) -> None:
    spark = get_spark()
    storage_path = storage_path or f"{config.local_data_path}/processed"

    log.info("=== Bronze Ingestion Pipeline ===")
    log.info("Reading from: %s", storage_path)

    raw_dfs = read_bronze_blobs(spark, f"{config.local_data_path}/raw")

    for source, df in raw_dfs.items():
        log.info("Processing source: %s", source)

        # Add lineage metadata
        df = add_bronze_metadata(df, source)

        # Schema validation
        valid_df, invalid_df = validate_bronze_schema(df, source)

        # Write valid records to bronze partitioned tables
        partition_cols = ["event_date", "event_hour", "store_id"] if "store_id" in valid_df.columns \
                         else ["event_date", "event_hour"]
        write_bronze_table(valid_df, storage_path, source, partition_cols)

        # Quarantine invalid records
        write_quarantine(invalid_df, storage_path, source)

    log.info("=== Bronze Pipeline Complete ===")
    spark.stop()


if __name__ == "__main__":
    run_bronze_pipeline()
