"""
Bronze Layer — Transaction Batch Ingestion
-------------------------------------------
Ingests daily transaction feeds from the core banking system (CBS) into the
Bronze zone of ADLS Gen2 as raw Delta tables.  No business transformations
are applied here — raw fidelity is preserved for audit and reprocessing.

Source   : Azure SQL / CBS nightly export (Parquet files landed in ADLS)
Target   : Bronze Delta table — financial/transactions
Schedule : Daily 01:00 UTC via ADF trigger
"""

import logging
from datetime import datetime, date
from typing import Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, DateType, IntegerType, BooleanType
)
from delta.tables import DeltaTable

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Source schema  (matches the CBS nightly Parquet export spec v2.4)
# ---------------------------------------------------------------------------

TRANSACTION_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),    nullable=False),
    StructField("account_number",       StringType(),    nullable=False),
    StructField("customer_id",          StringType(),    nullable=False),
    StructField("transaction_datetime", TimestampType(), nullable=False),
    StructField("transaction_date",     DateType(),      nullable=False),
    StructField("transaction_type",     StringType(),    nullable=True),   # CREDIT | DEBIT | TRANSFER
    StructField("transaction_channel",  StringType(),    nullable=True),   # BRANCH | ATM | ONLINE | MOBILE
    StructField("amount_usd",           DoubleType(),    nullable=False),
    StructField("currency_code",        StringType(),    nullable=True),
    StructField("originating_country",  StringType(),    nullable=True),
    StructField("merchant_category_code", StringType(), nullable=True),
    StructField("merchant_name",        StringType(),    nullable=True),
    StructField("is_international",     BooleanType(),   nullable=True),
    StructField("account_balance_usd",  DoubleType(),    nullable=True),
    StructField("branch_code",          StringType(),    nullable=True),
    StructField("source_system",        StringType(),    nullable=True),
    StructField("batch_file_name",      StringType(),    nullable=True),
])


# ---------------------------------------------------------------------------
# Ingestion class
# ---------------------------------------------------------------------------

class TransactionBronzeIngestion:
    """
    Handles idempotent daily ingestion of CBS transaction exports into the
    Bronze Delta table.  Uses MERGE (upsert) on transaction_id to avoid
    duplicates on pipeline reruns.
    """

    def __init__(self, spark: SparkSession, ingestion_date: Optional[date] = None):
        self.spark = spark
        self.ingestion_date = ingestion_date or date.today()
        self.storage = config.storage
        self.target_table = self.storage.bronze_path("financial", "transactions")
        self.source_path  = (
            f"{self.storage.bronze_path('landing', 'cbs_export')}"
            f"/dt={self.ingestion_date.strftime('%Y-%m-%d')}"
        )

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_source(self) -> DataFrame:
        """
        Read the CBS nightly Parquet export from the landing zone.
        Adds pipeline metadata columns before any further processing.
        """
        log.info("Reading CBS export from: %s", self.source_path)

        df = (
            self.spark.read
            .schema(TRANSACTION_SCHEMA)
            .parquet(self.source_path)
        )

        record_count = df.count()
        log.info("Source records loaded: %d", record_count)

        # Attach ingestion metadata
        df = df.withColumns({
            "_ingestion_timestamp": F.current_timestamp(),
            "_ingestion_date":      F.lit(str(self.ingestion_date)),
            "_pipeline_version":    F.lit("2.4.0"),
            "_source_system":       F.lit("CBS_NIGHTLY_EXPORT"),
        })

        return df

    # ------------------------------------------------------------------
    # Validate
    # ------------------------------------------------------------------

    def validate(self, df: DataFrame) -> DataFrame:
        """
        Bronze-layer validation: reject records that would violate NOT NULL
        constraints or fail basic sanity checks.  Rejected rows are written
        to a quarantine table for manual review.
        """
        total = df.count()

        # Flag bad records (don't drop — preserve for audit)
        df = df.withColumn(
            "_is_quarantined",
            F.when(F.col("transaction_id").isNull(),   F.lit(True))
             .when(F.col("account_number").isNull(),   F.lit(True))
             .when(F.col("amount_usd").isNull(),        F.lit(True))
             .when(F.col("amount_usd") < 0,             F.lit(True))   # negative amounts flagged
             .otherwise(F.lit(False))
        )

        quarantine_count = df.filter(F.col("_is_quarantined")).count()
        clean_count      = total - quarantine_count

        log.info("Validation — Total: %d | Clean: %d | Quarantined: %d",
                 total, clean_count, quarantine_count)

        # Write quarantine partition
        if quarantine_count > 0:
            quarantine_path = self.storage.bronze_path("financial", "transactions_quarantine")
            (
                df.filter(F.col("_is_quarantined"))
                  .write
                  .format("delta")
                  .mode("append")
                  .partitionBy("_ingestion_date")
                  .save(quarantine_path)
            )
            log.warning("Quarantined %d records written to: %s", quarantine_count, quarantine_path)

        return df.filter(~F.col("_is_quarantined")).drop("_is_quarantined")

    # ------------------------------------------------------------------
    # Write (upsert)
    # ------------------------------------------------------------------

    def upsert(self, df: DataFrame) -> None:
        """
        Merge incoming records into the Bronze Delta table using transaction_id
        as the natural key.  Idempotent — safe to rerun for the same date.
        """
        if DeltaTable.isDeltaTable(self.spark, self.target_table):
            log.info("Target Delta table exists — performing MERGE upsert.")
            delta_table = DeltaTable.forPath(self.spark, self.target_table)

            (
                delta_table.alias("target")
                .merge(
                    df.alias("source"),
                    "target.transaction_id = source.transaction_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            log.info("Target Delta table not found — creating with initial load.")
            (
                df.write
                  .format("delta")
                  .mode("overwrite")
                  .partitionBy("transaction_date")
                  .option("delta.autoOptimize.optimizeWrite", "true")
                  .option("delta.autoOptimize.autoCompact",   "true")
                  .save(self.target_table)
            )

        log.info("Upsert complete. Target: %s", self.target_table)

    # ------------------------------------------------------------------
    # Orchestrate
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info("=== Bronze Transaction Ingestion | Date: %s ===", self.ingestion_date)
        try:
            raw_df    = self.read_source()
            clean_df  = self.validate(raw_df)
            self.upsert(clean_df)
            log.info("=== Ingestion completed successfully ===")
        except Exception as exc:
            log.error("Ingestion failed: %s", str(exc), exc_info=True)
            raise


# ---------------------------------------------------------------------------
# Entry point (called by ADF / Databricks job)
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(config.spark.app_name)
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled",   str(config.spark.adaptive_enabled).lower())
        .config("spark.databricks.delta.schema.autoMerge.enabled", "true")
        .getOrCreate()
    )

    ingestion_date = datetime.strptime(
        spark.conf.get("pipeline.ingestion_date", str(date.today())), "%Y-%m-%d"
    ).date()

    TransactionBronzeIngestion(spark, ingestion_date).run()

