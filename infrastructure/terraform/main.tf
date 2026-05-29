##############################################################################
# Terraform — Risk Analytics Lakehouse Infrastructure
# ----------------------------------------------------
# Provisions all Azure resources required for the Risk Analytics Lakehouse:
#   - ADLS Gen2 storage account (Bronze / Silver / Gold zones)
#   - Azure Databricks workspace
#   - Azure Data Factory
#   - Azure Event Hubs namespace + hub
#   - Azure Key Vault (secrets management)
#   - Azure Purview account (data governance & lineage)
#
# Usage:
#   terraform init
#   terraform plan  -var-file="environments/dev.tfvars"
#   terraform apply -var-file="environments/dev.tfvars"
##############################################################################

terraform {
  required_version = ">= 1.5.0"

  required_providers {
    azurerm = {
      source  = "hashicorp/azurerm"
      version = "~> 3.85"
    }
  }

  backend "azurerm" {
    resource_group_name  = "rg-risk-analytics-tfstate"
    storage_account_name = "risktfstatestore"
    container_name       = "tfstate"
    key                  = "risk-lakehouse.tfstate"
  }
}

provider "azurerm" {
  features {
    key_vault {
      purge_soft_delete_on_destroy    = false
      recover_soft_deleted_key_vaults = true
    }
  }
}


##############################################################################
# Variables
##############################################################################

variable "environment" {
  description = "Deployment environment (dev | staging | prod)"
  type        = string
  validation {
    condition     = contains(["dev", "staging", "prod"], var.environment)
    error_message = "environment must be dev, staging, or prod."
  }
}

variable "location" {
  description = "Azure region for all resources"
  type        = string
  default     = "eastus2"
}

variable "tags" {
  description = "Tags applied to all resources for cost allocation and governance"
  type        = map(string)
  default = {
    project    = "risk-analytics-lakehouse"
    managed_by = "terraform"
    team       = "data-engineering"
  }
}

variable "databricks_sku" {
  description = "Databricks workspace SKU"
  type        = string
  default     = "premium"   # premium required for Unity Catalog
}

variable "adls_replication" {
  description = "ADLS Gen2 replication type"
  type        = string
  default     = "ZRS"       # Zone-redundant for production resilience
}


##############################################################################
# Resource group
##############################################################################

resource "azurerm_resource_group" "rg" {
  name     = "rg-risk-analytics-${var.environment}"
  location = var.location
  tags     = merge(var.tags, { environment = var.environment })
}


##############################################################################
# ADLS Gen2 — Lakehouse storage
##############################################################################

resource "azurerm_storage_account" "lakehouse" {
  name                      = "risklakehouse${var.environment}"
  resource_group_name       = azurerm_resource_group.rg.name
  location                  = var.location
  account_tier              = "Standard"
  account_replication_type  = var.adls_replication
  account_kind              = "StorageV2"
  is_hns_enabled            = true    # Hierarchical namespace = ADLS Gen2

  # Security hardening
  min_tls_version           = "TLS1_2"
  enable_https_traffic_only = true
  allow_nested_items_to_be_public = false

  blob_properties {
    versioning_enabled       = true
    change_feed_enabled      = true
    last_access_time_enabled = true   # lifecycle management

    delete_retention_policy {
      days = 30
    }
    container_delete_retention_policy {
      days = 30
    }
  }

  tags = merge(var.tags, { environment = var.environment, tier = "storage" })
}

# Medallion zone containers
resource "azurerm_storage_container" "lakehouse" {
  name                  = "lakehouse"
  storage_account_name  = azurerm_storage_account.lakehouse.name
  container_access_type = "private"
}


##############################################################################
# Azure Databricks workspace
##############################################################################

resource "azurerm_databricks_workspace" "databricks" {
  name                = "adb-risk-analytics-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  sku                 = var.databricks_sku

  custom_parameters {
    no_public_ip = true    # VNet injection — no public cluster IPs
  }

  tags = merge(var.tags, { environment = var.environment, tier = "compute" })
}


##############################################################################
# Azure Data Factory
##############################################################################

resource "azurerm_data_factory" "adf" {
  name                = "adf-risk-analytics-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  # Git integration (Repos-based CI/CD for pipeline definitions)
  github_configuration {
    account_name    = "risk-data-engineering"
    branch_name     = var.environment == "prod" ? "main" : var.environment
    git_url         = "https://github.com"
    repository_name = "enterprise-risk-analytics-lakehouse"
    root_folder     = "/adf"
  }

  tags = merge(var.tags, { environment = var.environment, tier = "integration" })
}


##############################################################################
# Azure Event Hubs — real-time transaction event streaming
##############################################################################

resource "azurerm_eventhub_namespace" "eh_ns" {
  name                = "riskanalytics-eh-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location
  sku                 = var.environment == "prod" ? "Standard" : "Basic"
  capacity            = var.environment == "prod" ? 4 : 1   # throughput units

  auto_inflate_enabled     = var.environment == "prod"
  maximum_throughput_units = var.environment == "prod" ? 20 : null

  tags = merge(var.tags, { environment = var.environment, tier = "streaming" })
}

resource "azurerm_eventhub" "transactions" {
  name                = "transaction-events"
  namespace_name      = azurerm_eventhub_namespace.eh_ns.name
  resource_group_name = azurerm_resource_group.rg.name
  partition_count     = var.environment == "prod" ? 32 : 4
  message_retention   = 7   # days
}

resource "azurerm_eventhub_consumer_group" "spark_consumer" {
  name                = "spark-structured-streaming"
  namespace_name      = azurerm_eventhub_namespace.eh_ns.name
  eventhub_name       = azurerm_eventhub.transactions.name
  resource_group_name = azurerm_resource_group.rg.name
}


##############################################################################
# Azure Key Vault — secrets management
##############################################################################

data "azurerm_client_config" "current" {}

resource "azurerm_key_vault" "kv" {
  name                      = "kv-risk-analytics-${var.environment}"
  resource_group_name       = azurerm_resource_group.rg.name
  location                  = var.location
  tenant_id                 = data.azurerm_client_config.current.tenant_id
  sku_name                  = "standard"
  purge_protection_enabled  = true    # required for regulatory environments
  soft_delete_retention_days = 90

  network_acls {
    default_action = "Deny"
    bypass         = "AzureServices"
  }

  tags = merge(var.tags, { environment = var.environment, tier = "security" })
}


##############################################################################
# Azure Purview — data governance & lineage
##############################################################################

resource "azurerm_purview_account" "purview" {
  name                = "purview-risk-analytics-${var.environment}"
  resource_group_name = azurerm_resource_group.rg.name
  location            = var.location

  identity {
    type = "SystemAssigned"
  }

  tags = merge(var.tags, { environment = var.environment, tier = "governance" })
}


##############################################################################
# RBAC — grant ADF managed identity access to ADLS
##############################################################################

resource "azurerm_role_assignment" "adf_to_adls" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Contributor"
  principal_id         = azurerm_data_factory.adf.identity[0].principal_id
}

resource "azurerm_role_assignment" "purview_to_adls" {
  scope                = azurerm_storage_account.lakehouse.id
  role_definition_name = "Storage Blob Data Reader"
  principal_id         = azurerm_purview_account.purview.identity[0].principal_id
}


##############################################################################
# Outputs
##############################################################################

output "adls_storage_account_name" {
  description = "ADLS Gen2 storage account name for pipeline config"
  value       = azurerm_storage_account.lakehouse.name
}

output "databricks_workspace_url" {
  description = "Databricks workspace URL"
  value       = "https://${azurerm_databricks_workspace.databricks.workspace_url}"
}

output "event_hub_namespace" {
  description = "Event Hubs namespace for pipeline config"
  value       = azurerm_eventhub_namespace.eh_ns.name
}

output "key_vault_uri" {
  description = "Key Vault URI for Databricks secret scope configuration"
  value       = azurerm_key_vault.kv.vault_uri
  sensitive   = true
}

output "purview_account_name" {
  description = "Purview account name for lineage configuration"
  value       = azurerm_purview_account.purview.name
}

