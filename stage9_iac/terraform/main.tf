# Stage 9: Terraform IaC — Azure Retail Data Platform
# Provisions all Azure resources using free/low-cost tiers.
#
# Usage:
#   terraform init
#   terraform plan -var-file="dev.tfvars"
#   terraform apply -var-file="dev.tfvars"

terraform {
  required_version = ">= 1.6.0"
  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
  }
  # Remote state — use Azure Blob for free
  backend "azurerm" {
    resource_group_name  = "rg-retail-platform-tfstate"
    storage_account_name = "retailplatformtfstate"
    container_name       = "tfstate"
    key                  = "retail-platform.terraform.tfstate"
  }
}

provider "azurerm" {
  features {
    resource_group {
      prevent_deletion_if_contains_resources = false
    }
  }
}

# ─── Resource Group ───────────────────────────────────────────────────────────

resource "azurerm_resource_group" "main" {
  name     = "rg-retail-platform-${var.environment}"
  location = var.location
  tags     = local.common_tags
}

# ─── Azure Data Lake Storage Gen2 (free 5GB 12mo) ────────────────────────────

resource "azurerm_storage_account" "datalake" {
  name                     = "retaildatalake${var.environment}${random_string.suffix.result}"
  resource_group_name      = azurerm_resource_group.main.name
  location                 = var.location
  account_tier             = "Standard"
  account_replication_type = "LRS"             # Cheapest — use ZRS/GRS in prod
  account_kind             = "StorageV2"
  is_hns_enabled           = true              # Enables ADLS Gen2 hierarchical namespace
  min_tls_version          = "TLS1_2"

  blob_properties {
    delete_retention_policy {
      days = 7
    }
    versioning_enabled = false                 # Disable to save costs
  }

  tags = local.common_tags
}

# Bronze / Silver / Gold containers
resource "azurerm_storage_container" "bronze" {
  name                  = "bronze"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "silver" {
  name                  = "silver"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "gold" {
  name                  = "gold"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

resource "azurerm_storage_container" "checkpoints" {
  name                  = "checkpoints"
  storage_account_name  = azurerm_storage_account.datalake.name
  container_access_type = "private"
}

# ─── Azure Event Hubs (Basic — cheapest tier) ─────────────────────────────────

resource "azurerm_eventhub_namespace" "retail" {
  name                = "retail-eventhub-${var.environment}-${random_string.suffix.result}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "Basic"               # Free namespace, pay per million events
  capacity            = 1
  tags                = local.common_tags
}

resource "azurerm_eventhub" "pos" {
  name                = "pos-events"
  namespace_name      = azurerm_eventhub_namespace.retail.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4                     # Basic tier: max 4 partitions
  message_retention   = 1                     # Basic tier: 1 day retention
}

resource "azurerm_eventhub" "inventory" {
  name                = "inventory-events"
  namespace_name      = azurerm_eventhub_namespace.retail.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

resource "azurerm_eventhub" "ecommerce" {
  name                = "ecommerce-events"
  namespace_name      = azurerm_eventhub_namespace.retail.name
  resource_group_name = azurerm_resource_group.main.name
  partition_count     = 4
  message_retention   = 1
}

# ─── Azure Functions (Consumption plan — free 1M executions/month) ────────────

resource "azurerm_service_plan" "functions" {
  name                = "asp-retail-functions-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  os_type             = "Linux"
  sku_name            = "Y1"                  # Consumption plan — pay per execution
  tags                = local.common_tags
}

resource "azurerm_linux_function_app" "retail_triggers" {
  name                       = "retail-triggers-${var.environment}-${random_string.suffix.result}"
  resource_group_name        = azurerm_resource_group.main.name
  location                   = var.location
  service_plan_id            = azurerm_service_plan.functions.id
  storage_account_name       = azurerm_storage_account.datalake.name
  storage_account_access_key = azurerm_storage_account.datalake.primary_access_key

  site_config {
    application_stack {
      python_version = "3.11"
    }
  }

  app_settings = {
    "FUNCTIONS_WORKER_RUNTIME"        = "python"
    "AZURE_STORAGE_CONNECTION_STRING" = azurerm_storage_account.datalake.primary_connection_string
    "EVENT_HUB_CONNECTION_STRING"     = azurerm_eventhub_namespace.retail.default_primary_connection_string
    "ENVIRONMENT"                     = var.environment
  }

  tags = local.common_tags
}

# ─── Azure Monitor Log Analytics (free 5GB/month) ────────────────────────────

resource "azurerm_log_analytics_workspace" "retail" {
  name                = "law-retail-${var.environment}"
  resource_group_name = azurerm_resource_group.main.name
  location            = var.location
  sku                 = "PerGB2018"
  retention_in_days   = 30                    # Free up to 31 days
  tags                = local.common_tags
}

# ─── Key Vault (free tier — ~$0.03/10k operations) ───────────────────────────

resource "azurerm_key_vault" "retail" {
  name                        = "kv-retail-${var.environment}-${random_string.suffix.result}"
  resource_group_name         = azurerm_resource_group.main.name
  location                    = var.location
  enabled_for_disk_encryption = false
  tenant_id                   = data.azurerm_client_config.current.tenant_id
  sku_name                    = "standard"
  soft_delete_retention_days  = 7
  tags                        = local.common_tags
}

# ─── Locals & data sources ────────────────────────────────────────────────────

locals {
  common_tags = {
    project     = "retail-data-platform"
    environment = var.environment
    managed_by  = "terraform"
  }
}

data "azurerm_client_config" "current" {}

resource "random_string" "suffix" {
  length  = 6
  special = false
  upper   = false
}

# ─── Outputs ──────────────────────────────────────────────────────────────────

output "storage_account_name" {
  value = azurerm_storage_account.datalake.name
}

output "storage_connection_string" {
  value     = azurerm_storage_account.datalake.primary_connection_string
  sensitive = true
}

output "eventhub_connection_string" {
  value     = azurerm_eventhub_namespace.retail.default_primary_connection_string
  sensitive = true
}

output "function_app_hostname" {
  value = azurerm_linux_function_app.retail_triggers.default_hostname
}
