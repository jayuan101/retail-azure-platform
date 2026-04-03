"""
Stage 5: Gold Layer Aggregations
Builds business-facing retail datasets for reporting (sub-second queries, <5-min SLA):
  - Daily sales summary by store
  - Hourly sales trend
  - Inventory snapshot (current stock levels by SKU/store)
  - Low-stock alert table
  - Top-selling products

Designed to load into Synapse Analytics dedicated SQL pool or serverless SQL.
"""

import logging
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "../.."))

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.window import Window
from stage1_infrastructure.config.settings import config
from stage3_bronze.scripts.bronze_ingestion import get_spark

log = logging.getLogger(__name__)
logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")


def daily_sales_summary(silver_pos: DataFrame) -> DataFrame:
    """
    Gold: daily_sales_by_store
    Enables sub-second reporting queries for store performance dashboards.
    """
    return (
        silver_pos
        .filter(F.col("event_type") == "sale")
        .filter(F.col("_is_valid") == True)
        .groupBy("event_date", "store_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("total_amount").alias("gross_revenue"),
            F.sum("discount_total").alias("total_discounts"),
            F.sum("tax_amount").alias("total_tax"),
            F.avg("total_amount").alias("avg_transaction_value"),
            F.max("total_amount").alias("max_transaction_value"),
            F.sum("item_count").alias("total_items_sold"),
            F.countDistinct("cashier_id").alias("unique_cashiers"),
            F.countDistinct("payment_method").alias("payment_methods_used"),
        )
        .withColumn("net_revenue", F.col("gross_revenue") - F.col("total_discounts"))
        .withColumn("_gold_ts", F.current_timestamp())
        .orderBy("event_date", "store_id")
    )


def hourly_sales_trend(silver_pos: DataFrame) -> DataFrame:
    """
    Gold: hourly_sales_trend
    Powers real-time dashboards and anomaly detection for peak event handling.
    """
    return (
        silver_pos
        .filter(F.col("event_type") == "sale")
        .groupBy("event_date", "event_hour", "store_id")
        .agg(
            F.count("transaction_id").alias("transaction_count"),
            F.sum("total_amount").alias("hourly_revenue"),
            F.avg("total_amount").alias("avg_basket_size"),
        )
        .withColumn(
            "prev_hour_revenue",
            F.lag("hourly_revenue", 1).over(
                Window.partitionBy("event_date", "store_id")
                      .orderBy("event_hour")
            )
        )
        .withColumn(
            "revenue_change_pct",
            F.when(F.col("prev_hour_revenue") > 0,
                   (F.col("hourly_revenue") - F.col("prev_hour_revenue")) / F.col("prev_hour_revenue") * 100
            ).otherwise(None)
        )
        .withColumn("_gold_ts", F.current_timestamp())
    )


def inventory_snapshot(silver_inventory: DataFrame) -> DataFrame:
    """
    Gold: inventory_current_snapshot
    Latest on-hand quantity per SKU per store — serves inventory dashboards.
    Uses window function to get most recent event per SKU/store combination.
    """
    window = (
        Window.partitionBy("store_id", "sku")
              .orderBy(F.col("event_ts").desc())
    )
    return (
        silver_inventory
        .withColumn("_rn", F.row_number().over(window))
        .filter(F.col("_rn") == 1)
        .select(
            "store_id", "sku", "quantity_on_hand", "reorder_point",
            "below_reorder", "location", "event_ts",
        )
        .withColumn("days_of_stock_remaining",
            F.when(F.col("quantity_on_hand") > 0, F.col("quantity_on_hand") / 10).otherwise(0)
        )
        .withColumn("_gold_ts", F.current_timestamp())
    )


def low_stock_alerts(inventory_snap: DataFrame) -> DataFrame:
    """
    Gold: low_stock_alerts
    Feeds Azure Functions and ADF alert pipelines.
    """
    return (
        inventory_snap
        .filter(F.col("below_reorder") == True)
        .select(
            "store_id", "sku", "quantity_on_hand", "reorder_point",
            "location", "_gold_ts"
        )
        .withColumn(
            "urgency",
            F.when(F.col("quantity_on_hand") == 0, "CRITICAL")
             .when(F.col("quantity_on_hand") < F.col("reorder_point") * 0.25, "HIGH")
             .otherwise("MEDIUM")
        )
        .orderBy("urgency", "quantity_on_hand")
    )


def top_selling_products(silver_pos: DataFrame, top_n: int = 20) -> DataFrame:
    """
    Gold: top_selling_products
    SKU-level sales ranking — drives merchandising and pricing decisions.
    """
    # Parse items JSON array to explode line items
    items_schema = "array<struct<sku:string,product_name:string,category:string,quantity:int,unit_price:double,line_total:double>>"

    # Note: in bronze, items is JSON string — parse it back
    exploded = (
        silver_pos
        .filter(F.col("event_type") == "sale")
        .withColumn("items_parsed", F.from_json(F.col("items") if "items" in silver_pos.columns else F.lit("[]"), items_schema))
        .withColumn("item", F.explode("items_parsed"))
        .select(
            "event_date",
            F.col("item.sku").alias("sku"),
            F.col("item.product_name").alias("product_name"),
            F.col("item.category").alias("category"),
            F.col("item.quantity").alias("quantity_sold"),
            F.col("item.line_total").alias("line_revenue"),
        )
    )

    ranked = (
        exploded
        .groupBy("event_date", "sku", "product_name", "category")
        .agg(
            F.sum("quantity_sold").alias("total_units_sold"),
            F.sum("line_revenue").alias("total_revenue"),
        )
        .withColumn(
            "revenue_rank",
            F.rank().over(Window.partitionBy("event_date").orderBy(F.col("total_revenue").desc()))
        )
        .filter(F.col("revenue_rank") <= top_n)
        .withColumn("_gold_ts", F.current_timestamp())
    )
    return ranked


def write_gold(df: DataFrame, name: str, gold_path: str, partition_cols: list[str] = None) -> None:
    dest = f"{gold_path}/{name}"
    writer = df.write.mode("overwrite")
    if partition_cols:
        writer = writer.partitionBy(*partition_cols)
    writer.parquet(dest)
    log.info("Written gold.%s → %s", name, dest)


def run_gold_pipeline(storage_path: str = None) -> None:
    spark = get_spark()
    silver_path = f"{config.local_data_path}/silver"
    gold_path   = f"{config.local_data_path}/gold"

    log.info("=== Gold Aggregation Pipeline ===")

    try:
        silver_pos = spark.read.parquet(f"{silver_path}/silver_pos")
        silver_inv = spark.read.parquet(f"{silver_path}/silver_inventory")
    except Exception as exc:
        log.error("Could not read silver data — run silver pipeline first. %s", exc)
        spark.stop()
        return

    # Build and write gold tables
    write_gold(daily_sales_summary(silver_pos),   "daily_sales_by_store",  gold_path, ["event_date"])
    write_gold(hourly_sales_trend(silver_pos),    "hourly_sales_trend",    gold_path, ["event_date", "store_id"])
    inv_snap = inventory_snapshot(silver_inv)
    write_gold(inv_snap,                          "inventory_snapshot",    gold_path, ["store_id"])
    write_gold(low_stock_alerts(inv_snap),        "low_stock_alerts",      gold_path)
    write_gold(top_selling_products(silver_pos),  "top_selling_products",  gold_path, ["event_date"])

    # Register gold tables in Hive Metastore (Databricks)
    try:
        spark.sql("CREATE DATABASE IF NOT EXISTS retail_gold")
        for table in ["daily_sales_by_store", "hourly_sales_trend", "inventory_snapshot",
                      "low_stock_alerts", "top_selling_products"]:
            spark.sql(f"""
                CREATE TABLE IF NOT EXISTS retail_gold.{table}
                USING PARQUET
                LOCATION '{gold_path}/{table}'
            """)
            log.info("Registered Hive gold table: retail_gold.%s", table)
    except Exception as exc:
        log.warning("Hive gold registration skipped: %s", exc)

    log.info("=== Gold Pipeline Complete ===")
    spark.stop()


if __name__ == "__main__":
    run_gold_pipeline()
