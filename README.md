
# retail-azure-platform
# Real-Time Retail Data Platform — Azure Free Tier

A production-grade retail data platform built with Azure free-tier and open-source tools.
Simulates processing >150K events/sec across POS, e-commerce, and inventory systems.

## Architecture

```
POS / E-Commerce / Inventory
        │
        ▼
  [Kafka / Event Hubs]   ← Stage 1-2: Ingestion
        │
        ▼
  [Bronze Layer]          ← Stage 3: Raw landing in Azure Blob / ADLS
  (Azure Data Lake)
        │
        ▼
  [Silver Layer]          ← Stage 4: Cleaned + schema-enforced (Databricks)
  (Hive Metastore)
        │
        ▼
  [Gold Layer]            ← Stage 5: Aggregated retail models (Synapse)
  (Synapse / Delta)
        │
     ┌──┴──┐
  Airflow  ADF            ← Stage 6: Orchestration
     │
  Azure Functions          ← Stage 7: Event-driven automation
     │
  Terraform / ARM          ← Stage 8: IaC
     │
  Azure Monitor            ← Stage 9: Dashboards & alerting
```

## Free-Tier Service Map

| Resume Tech        | Free Equivalent Used                          |
|--------------------|-----------------------------------------------|
| Azure Event Hubs   | Event Hubs Basic + local Kafka (Docker)       |
| Databricks         | Databricks Community Edition                  |
| Azure Blob Storage | Azure free account (5 GB, 12 mo)              |
| Synapse Analytics  | Synapse serverless SQL (pay-per-query ~free)  |
| Apache Airflow     | Local Airflow (pip install apache-airflow)    |
| Azure Functions    | Free tier (1M executions/month)               |
| Azure Monitor      | Basic free metrics                            |
| Hive Metastore     | Databricks Community built-in metastore       |
| Terraform          | Free CLI — provision real Azure resources     |
| Kafka              | Docker local or Confluent Cloud free tier     |

## Stages

| Stage | Description |
|-------|-------------|
| 1 | Infrastructure — Kafka config, Event Hubs setup, project config |
| 2 | Ingestion — POS/inventory/e-commerce event producers & consumers |
| 3 | Bronze Layer — raw landing, schema detection, blob storage |
| 4 | Silver Layer — cleaning, deduplication, Hive schema governance |
| 5 | Gold Layer — sales aggregates, inventory snapshots, pricing models |
| 6 | Validation — Python utilities, SKU/item API ingestion, reconciliation |
| 7 | Orchestration — Airflow DAGs, ADF pipeline templates |
| 8 | Azure Functions — event-driven POS/SKU/inventory triggers |
| 9 | IaC — Terraform modules + ARM templates |
| 10 | Monitoring — Azure Monitor dashboards, lag alerts, cost tracking |

## Quick Start

```bash
# 1. Install dependencies
pip install -r requirements.txt

# 2. Start local Kafka (Docker)
docker-compose -f stage1_infrastructure/docker-compose.yml up -d

# 3. Generate sample events
python stage2_ingestion/producers/pos_producer.py --events 1000

# 4. Run bronze ingestion
python stage3_bronze/scripts/bronze_ingestion.py

# 5. Start Airflow (local)
airflow standalone
```

## Requirements

- Python 3.9+
- Docker Desktop (for local Kafka)
- Azure free account (storage + functions)
- Databricks Community Edition account (free)
>>>>>>> ddb11f8 (feat: initial commit — real-time retail data platform (Azure free tier))
