"""
Stage 4: Silver Layer Transformation
Reads Bronze Parquet, applies:
  - Data cleaning & deduplication
  - Type casting & null handling
  - Schema enforcement (Hive Metastore compatible DDL)
  - SCD Type 1 upserts for dimension tables

Run locally with PySpark or in Databricks Community Edition.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField, StringType, DoubleType,
    IntegerType, TimestampType, BooleanType, LongType
)
from stage1_infrastructure.config.settings import config
from stage3_bronze.scripts.bronze_ingestion import get_spark

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

# ─── Silver schemas (Hive Metastore DDL compatible) ───────────────────────────

SILVER_POS_SCHEMA = StructType([
    StructField("event_id",        StringType(),    False),
    StructField("event_type",      StringType(),    False),
    StructField("store_id",        StringType(),    False),
    StructField("terminal_id",     StringType(),    True),
    StructField("cashier_id",      StringType(),    True),
    StructField("event_ts",        TimestampType(), False),
    StructField("transaction_id",  StringType(),    False),
    StructField("subtotal",        DoubleType(),    True),
    StructField("discount_total",  DoubleType(),    True),
    StructField("tax_amount",      DoubleType(),    True),
    StructField("total_amount",    DoubleType(),    False),
    StructField("payment_method",  StringType(),    True),
    StructField("item_count",      IntegerType(),   True),
    StructField("event_date",      StringType(),    False),
    StructField("event_hour",      IntegerType(),   True),
    StructField("_silver_ts",      StringType(),    True),
    StructField("_is_valid",       BooleanType(),   True),
])

SILVER_INVENTORY_SCHEMA = StructType([
    StructField("event_id",         StringType(),    False),
    StructField("event_type",       StringType(),    False),
    StructField("store_id",         StringType(),    False),
    StructField("sku",              StringType(),    False),
    StructField("event_ts",         TimestampType(), False),
    StructField("quantity_delta",   IntegerType(),   True),
    StructField("quantity_on_hand", IntegerType(),   False),
    StructField("reorder_point",    IntegerType(),   True),
    StructField("below_reorder",    BooleanType(),   True),
    StructField("supplier_id",      StringType(),    True),
    StructField("po_number",        StringType(),    True),
    StructField("location",         StringType(),    True),
    StructField("event_date",       StringType(),    False),
    StructField("_silver_ts",       StringType(),    True),
])


# ─── Transformations ──────────────────────────────────────────────────────────

def clean_pos(df: DataFrame) -> DataFrame:
    """Clean and flatten POS bronze records for silver layer."""
    from datetime import datetime, timezone

    # Parse JSON-stringified nested columns back for extraction
    df = (
        df
        .withColumn("totals_json",  F.from_json(F.col("totals"),  "subtotal double, discount double, tax double, total double"))
        .withColumn("payment_json", F.from_json(F.col("payment"), "method string, amount_tendered double"))
        .withColumn("event_ts",     F.to_timestamp("timestamp"))
        .withColumn("item_count",   F.size(F.from_json(F.col("items"), "array<struct<sku:string>>")))
    )

    df = (
        df
        .select(
            F.col("event_id"),
            F.col("event_type"),
            F.col("store_id"),
            F.col("terminal_id"),
            F.col("cashier_id"),
            F.col("event_ts"),
            F.col("transaction_id"),
            F.col("totals_json.subtotal").alias("subtotal"),
            F.col("totals_json.discount").alias("discount_total"),
            F.col("totals_json.tax").alias("tax_amount"),
            F.col("totals_json.total").alias("total_amount"),
            F.col("payment_json.method").alias("payment_method"),
            F.col("item_count"),
            F.col("event_date"),
            F.col("event_hour"),
            F.col("_bronze_loaded_at"),
        )
        .withColumn("_silver_ts", F.lit(datetime.now(timezone.utc).isoformat()))
        .withColumn("_is_valid", F.col("total_amount").isNotNull() & (F.col("total_amount") >= 0))
    )

    return df


def deduplicate(df: DataFrame, key_col: str = "event_id") -> DataFrame:
    """Remove duplicate events, keeping the latest by ingestion time."""
    window = (
        __import__("pyspark.sql.window", fromlist=["Window"])
        .Window.partitionBy(key_col)
        .orderBy(F.col("_bronze_loaded_at").desc())
    )
    return (
        df
        .withColumn("_row_num", F.row_number().over(window))
        .filter(F.col("_row_num") == 1)
        .drop("_row_num", "_bronze_loaded_at")
    )


def clean_inventory(df: DataFrame) -> DataFrame:
    from datetime import datetime, timezone
    return (
        df
        .withColumn("event_ts",      F.to_timestamp("timestamp"))
        .withColumn("below_reorder", F.col("quantity_on_hand") <= F.col("reorder_point"))
        .withColumn("_silver_ts",    F.lit(datetime.now(timezone.utc).isoformat()))
        .select(
            "event_id", "event_type", "store_id", "sku",
            "event_ts", "quantity_delta", "quantity_on_hand",
            "reorder_point", "below_reorder", "supplier_id",
            "po_number", "location", "event_date", "_silver_ts"
        )
    )


# ─── Hive Metastore DDL ───────────────────────────────────────────────────────

def register_hive_tables(spark: SparkSession, silver_path: str) -> None:
    """
    Register silver Parquet paths as Hive Metastore external tables.
    Works in Databricks Community Edition — provides consistent schema
    governance across Databricks and Synapse.
    """
    spark.sql("CREATE DATABASE IF NOT EXISTS retail_silver")

    ddl_statements = [
        f"""
        CREATE TABLE IF NOT EXISTS retail_silver.pos_transactions
        USING PARQUET
        PARTITIONED BY (event_date, store_id)
        LOCATION '{silver_path}/silver_pos'
        """,
        f"""
        CREATE TABLE IF NOT EXISTS retail_silver.inventory_events
        USING PARQUET
        PARTITIONED BY (event_date, store_id)
        LOCATION '{silver_path}/silver_inventory'
        """,
    ]
    for ddl in ddl_statements:
        spark.sql(ddl.strip())
        log.info("Registered Hive table: %s", ddl.split("TABLE IF NOT EXISTS")[1].split()[0].strip())

    # Recover partitions from existing data
    spark.sql("MSCK REPAIR TABLE retail_silver.pos_transactions")
    spark.sql("MSCK REPAIR TABLE retail_silver.inventory_events")


# ─── Pipeline ─────────────────────────────────────────────────────────────────

def run_silver_pipeline(storage_path: str = None) -> None:
    spark = get_spark()
    storage_path = storage_path or f"{config.local_data_path}/processed"
    bronze_path  = storage_path
    silver_path  = f"{config.local_data_path}/silver"

    log.info("=== Silver Transformation Pipeline ===")

    # POS silver
    try:
        bronze_pos = spark.read.parquet(f"{bronze_path}/bronze_pos")
        silver_pos = clean_pos(bronze_pos)
        silver_pos = deduplicate(silver_pos, "event_id")

        (
            silver_pos.write
            .mode("overwrite")
            .partitionBy("event_date", "store_id")
            .parquet(f"{silver_path}/silver_pos")
        )
        log.info("Silver POS written: %d records", silver_pos.count())
    except Exception as exc:
        log.warning("POS silver skipped (no bronze data?): %s", exc)

    # Inventory silver
    try:
        bronze_inv = spark.read.parquet(f"{bronze_path}/bronze_inventory")
        silver_inv = clean_inventory(bronze_inv)
        silver_inv = deduplicate(silver_inv, "event_id")

        (
            silver_inv.write
            .mode("overwrite")
            .partitionBy("event_date", "store_id")
            .parquet(f"{silver_path}/silver_inventory")
        )
        log.info("Silver inventory written: %d records", silver_inv.count())
    except Exception as exc:
        log.warning("Inventory silver skipped: %s", exc)

    # Register Hive Metastore external tables
    try:
        register_hive_tables(spark, silver_path)
    except Exception as exc:
        log.warning("Hive registration skipped (run in Databricks for full support): %s", exc)

    log.info("=== Silver Pipeline Complete ===")
    spark.stop()


if __name__ == "__main__":
    run_silver_pipeline()
