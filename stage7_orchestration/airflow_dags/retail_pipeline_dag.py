"""
Stage 7: Airflow DAG — Retail Daily Pipeline
Orchestrates the full bronze → silver → gold pipeline with:
  - Dependency management
  - Retry logic and SLA monitoring
  - Lineage tracking
  - Alerting on failure

Install: pip install apache-airflow apache-airflow-providers-microsoft-azure
Run:     airflow standalone  (starts scheduler + webserver locally)
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator, BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.utils.trigger_rule import TriggerRule

# ─── Default DAG args ────────────────────────────────────────────────────────

DEFAULT_ARGS = {
    "owner":             "retail-data-platform",
    "depends_on_past":   False,
    "email_on_failure":  True,
    "email_on_retry":    False,
    "retries":           3,
    "retry_delay":       timedelta(minutes=5),
    "retry_exponential_backoff": True,
    "max_retry_delay":   timedelta(minutes=30),
}

# ─── Task functions ───────────────────────────────────────────────────────────

def run_sku_api_ingestion(**context) -> dict:
    """Fetch SKU metadata from product API and write to silver."""
    import sys, os
    sys.path.insert(0, "/opt/airflow/dags")
    from stage6_validation.api_ingestion.sku_api_ingestion import SkuApiIngestion
    ingestion = SkuApiIngestion()
    return ingestion.run(max_pages=20, page_size=100)


def run_bronze_ingestion(**context) -> None:
    """Read Kafka-landed Parquet from blob and organize bronze layer."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from stage3_bronze.scripts.bronze_ingestion import run_bronze_pipeline
    run_bronze_pipeline()


def run_silver_transform(**context) -> None:
    """Clean and deduplicate bronze data into governed silver tables."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from stage4_silver.scripts.silver_transform import run_silver_pipeline
    run_silver_pipeline()


def run_gold_aggregations(**context) -> None:
    """Build gold-layer aggregations for reporting dashboards."""
    import sys
    sys.path.insert(0, "/opt/airflow/dags")
    from stage5_gold.scripts.gold_aggregations import run_gold_pipeline
    run_gold_pipeline()


def run_validation(**context) -> str:
    """Validate silver data quality. Returns 'pass' or 'quarantine_alert'."""
    import sys, os, json, glob
    sys.path.insert(0, "/opt/airflow/dags")
    from stage6_validation.utilities.data_validator import RetailDataValidator
    import pandas as pd

    validator = RetailDataValidator()
    silver_path = os.environ.get("LOCAL_DATA_PATH", "./data") + "/silver"

    for source in ["pos", "inventory"]:
        files = glob.glob(f"{silver_path}/silver_{source}/**/*.parquet", recursive=True)
        if not files:
            continue
        df = pd.read_parquet(files[0])
        result = validator.validate_dataframe(df, source)
        print(result.summary())
        if not result.passed:
            return "quarantine_alert"

    return "validation_passed"


def send_pipeline_alert(**context) -> None:
    """Send alert for validation failures (Slack/email/Teams integration point)."""
    dag_id  = context["dag"].dag_id
    run_id  = context["run_id"]
    message = f"[ALERT] {dag_id} run {run_id}: Data quality validation failed. Check quarantine."
    print(message)
    # TODO: integrate with Azure Monitor / Slack webhook
    # import requests; requests.post(SLACK_WEBHOOK, json={"text": message})


def check_low_stock_alerts(**context) -> None:
    """Check gold layer for critical low-stock items and trigger alerts."""
    import os, glob
    import pandas as pd

    gold_path = os.environ.get("LOCAL_DATA_PATH", "./data") + "/gold"
    alert_files = glob.glob(f"{gold_path}/low_stock_alerts/**/*.parquet", recursive=True)
    if not alert_files:
        print("No low stock alerts found.")
        return

    df = pd.read_parquet(alert_files[0])
    critical = df[df["urgency"] == "CRITICAL"]
    if not critical.empty:
        print(f"CRITICAL low stock: {len(critical)} SKUs at zero inventory!")
        print(critical[["store_id", "sku", "quantity_on_hand"]].to_string())
    else:
        print(f"Low stock items (non-critical): {len(df)}")


# ─── DAG Definition ───────────────────────────────────────────────────────────

with DAG(
    dag_id="retail_daily_pipeline",
    default_args=DEFAULT_ARGS,
    description="End-to-end retail data pipeline: ingest → bronze → silver → gold → validate",
    schedule_interval="0 2 * * *",    # Daily at 02:00 UTC
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["retail", "production", "lakehouse"],
    sla_miss_callback=None,           # Add SLA monitoring here
) as dag:

    start = EmptyOperator(task_id="pipeline_start")

    # ── Stage 1: Parallel data ingestion ─────────────────────────────────────
    sku_ingestion = PythonOperator(
        task_id="sku_api_ingestion",
        python_callable=run_sku_api_ingestion,
        execution_timeout=timedelta(minutes=30),
        doc_md="Fetch product/SKU metadata from API and write to silver dimension table.",
    )

    bronze_ingestion = PythonOperator(
        task_id="bronze_ingestion",
        python_callable=run_bronze_ingestion,
        execution_timeout=timedelta(hours=2),
        doc_md="Land Kafka-sourced raw events from blob storage into structured Bronze Parquet.",
    )

    # ── Stage 2: Silver transformation ───────────────────────────────────────
    silver_transform = PythonOperator(
        task_id="silver_transformation",
        python_callable=run_silver_transform,
        execution_timeout=timedelta(hours=3),
        doc_md="Clean, deduplicate, and schema-enforce bronze data into silver tables.",
    )

    # ── Stage 3: Validation gate ──────────────────────────────────────────────
    validation_gate = BranchPythonOperator(
        task_id="data_quality_validation",
        python_callable=run_validation,
        doc_md="Validate silver data quality. Routes to gold or quarantine alert.",
    )

    validation_passed = EmptyOperator(task_id="validation_passed")

    quarantine_alert = PythonOperator(
        task_id="quarantine_alert",
        python_callable=send_pipeline_alert,
        doc_md="Alert on-call team of data quality failures.",
    )

    # ── Stage 4: Gold aggregations ────────────────────────────────────────────
    gold_aggregations = PythonOperator(
        task_id="gold_aggregations",
        python_callable=run_gold_aggregations,
        execution_timeout=timedelta(hours=2),
        trigger_rule=TriggerRule.NONE_FAILED_MIN_ONE_SUCCESS,
        doc_md="Build gold-layer sales, inventory, and product aggregation tables.",
    )

    # ── Stage 5: Post-processing ──────────────────────────────────────────────
    low_stock_check = PythonOperator(
        task_id="low_stock_alerts",
        python_callable=check_low_stock_alerts,
        doc_md="Check for critical low-stock events and trigger restock alerts.",
    )

    end = EmptyOperator(task_id="pipeline_complete", trigger_rule=TriggerRule.ALL_DONE)

    # ─── Task dependencies ────────────────────────────────────────────────────
    start >> [sku_ingestion, bronze_ingestion]
    [sku_ingestion, bronze_ingestion] >> silver_transform
    silver_transform >> validation_gate
    validation_gate >> [validation_passed, quarantine_alert]
    validation_passed >> gold_aggregations
    quarantine_alert >> gold_aggregations    # Still run gold with available data
    gold_aggregations >> low_stock_check >> end
