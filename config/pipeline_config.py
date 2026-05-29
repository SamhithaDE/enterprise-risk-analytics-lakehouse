"""
Pipeline Configuration Manager
--------------------------------
Centralizes all environment-specific settings for the Risk Analytics
Lakehouse pipelines. Secrets are resolved from Azure Key Vault at runtime —
never hardcoded here.
"""

import os
from dataclasses import dataclass, field
from typing import Optional


# ---------------------------------------------------------------------------
# Storage
# ---------------------------------------------------------------------------

@dataclass
class StorageConfig:
    """ADLS Gen2 paths following the Bronze / Silver / Gold medallion layout."""

    storage_account: str = os.getenv("ADLS_STORAGE_ACCOUNT", "risklakehousedev")
    container: str = "lakehouse"

    # Medallion zones
    bronze_root: str = "abfss://lakehouse@{account}.dfs.core.windows.net/bronze"
    silver_root: str = "abfss://lakehouse@{account}.dfs.core.windows.net/silver"
    gold_root:   str = "abfss://lakehouse@{account}.dfs.core.windows.net/gold"

    def bronze_path(self, domain: str, table: str) -> str:
        return f"{self.bronze_root.format(account=self.storage_account)}/{domain}/{table}"

    def silver_path(self, domain: str, table: str) -> str:
        return f"{self.silver_root.format(account=self.storage_account)}/{domain}/{table}"

    def gold_path(self, domain: str, table: str) -> str:
        return f"{self.gold_root.format(account=self.storage_account)}/{domain}/{table}"


# ---------------------------------------------------------------------------
# Delta Lake
# ---------------------------------------------------------------------------

@dataclass
class DeltaConfig:
    """
    Delta Lake tuning parameters.

    target_file_size_mb  : Controls small-file compaction during OPTIMIZE.
    z_order_columns      : Columns used for multi-dimensional clustering.
                           Chosen to accelerate the most frequent query patterns
                           (account + date range lookups for risk reporting).
    retention_days       : Minimum snapshot retention for time-travel / audits.
                           Regulatory submissions require at least 7 years of
                           lineage, but Delta vacuum manages short-term snapshots.
    checkpoint_interval  : Streaming micro-batch checkpoint frequency.
    """
    target_file_size_mb: int = 128
    z_order_columns: list = field(default_factory=lambda: [
        "account_number", "transaction_date"
    ])
    retention_days: int = 30
    checkpoint_interval: int = 10   # batches


# ---------------------------------------------------------------------------
# Spark
# ---------------------------------------------------------------------------

@dataclass
class SparkConfig:
    """
    Databricks / Spark session tuning.

    Partition count is intentionally sized for 10 TB+ daily ingest on an
    autoscaling cluster.  Adaptive query execution is enabled to let Spark
    coalesce shuffle partitions at runtime.
    """
    app_name: str = "RiskAnalyticsLakehouse"
    shuffle_partitions: int = 400
    adaptive_enabled: bool = True
    broadcast_threshold_mb: int = 256
    max_records_per_file: int = 500_000

    # Databricks-specific
    cluster_autoscale_min: int = 4
    cluster_autoscale_max: int = 20
    worker_instance_type: str = "Standard_DS4_v2"


# ---------------------------------------------------------------------------
# Event Hubs (streaming ingestion)
# ---------------------------------------------------------------------------

@dataclass
class EventHubConfig:
    """
    Azure Event Hubs connection settings for the real-time transaction feed.
    Connection string is read from Key Vault via Databricks secret scope.
    """
    namespace: str = os.getenv("EH_NAMESPACE", "riskanalytics-eh-ns")
    hub_name: str = "transaction-events"
    consumer_group: str = "$Default"
    max_events_per_trigger: int = 50_000
    starting_offsets: str = "latest"
    secret_scope: str = "risk-analytics-kv"
    secret_key: str = "eventhub-connection-string"

    @property
    def connection_string_secret(self) -> str:
        return f"dbutils.secrets.get(scope='{self.secret_scope}', key='{self.secret_key}')"


# ---------------------------------------------------------------------------
# Risk thresholds  (business rules baked into pipeline logic)
# ---------------------------------------------------------------------------

@dataclass
class RiskThresholds:
    """
    Regulatory and internal risk classification thresholds.

    sar_reporting_usd    : Transactions at or above this amount are flagged for
                           Suspicious Activity Report (SAR) evaluation per FinCEN
                           guidelines (31 CFR 1020.320).
    high_risk_score      : Internal credit risk score above which an account is
                           routed to the enhanced due-diligence pipeline.
    velocity_window_hrs  : Rolling window for transaction velocity anomaly checks.
    velocity_max_count   : Maximum transactions allowed within the velocity window
                           before a velocity-breach flag is raised.
    """
    sar_reporting_usd: float   = 10_000.00
    high_risk_score: float     = 0.75
    velocity_window_hrs: int   = 24
    velocity_max_count: int    = 50


# ---------------------------------------------------------------------------
# Assembled pipeline config (single import point)
# ---------------------------------------------------------------------------

@dataclass
class PipelineConfig:
    environment: str = os.getenv("ENV", "dev")   # dev | staging | prod
    storage: StorageConfig     = field(default_factory=StorageConfig)
    delta: DeltaConfig         = field(default_factory=DeltaConfig)
    spark: SparkConfig         = field(default_factory=SparkConfig)
    event_hub: EventHubConfig  = field(default_factory=EventHubConfig)
    risk: RiskThresholds       = field(default_factory=RiskThresholds)

    def is_production(self) -> bool:
        return self.environment == "prod"


# Singleton used across pipeline modules
config = PipelineConfig()

