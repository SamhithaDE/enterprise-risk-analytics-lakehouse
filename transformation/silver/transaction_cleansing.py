"""
Silver Layer — Transaction Cleansing & Standardisation
-------------------------------------------------------
Transforms Bronze raw transactions into a clean, conformed Silver table.
Applies business rules, deduplication, data type standardisation, and
PII masking before the data is made available to Gold-layer consumers.

Source : Bronze Delta — financial/transactions
Target : Silver Delta — financial/transactions_clean
Trigger: Daily after Bronze ingestion completes (ADF dependency chain)
"""

import logging
from datetime import date
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from pyspark.sql.types import StringType
from delta.tables import DeltaTable

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Lookup maps (in production these are loaded from reference Delta tables)
# ---------------------------------------------------------------------------

# ISO 4217 currency codes accepted by the bank's processing system
ACCEPTED_CURRENCIES = {
    "USD", "EUR", "GBP", "CAD", "JPY", "AUD", "CHF", "HKD", "SGD", "MXN"
}

# MCC codes associated with elevated risk per internal risk policy
HIGH_RISK_MCC_CODES = {
    "6010",  # Financial institutions — manual cash disbursements
    "6011",  # Automated cash disbursements
    "7995",  # Gambling transactions
    "5912",  # Drug stores / pharmacies (cash-equivalent patterns)
    "6051",  # Non-financial institutions — foreign currency / money orders
}

# Standardise free-text channel values from CBS
CHANNEL_NORMALISATION = {
    "WEB": "ONLINE", "INTERNET": "ONLINE",
    "MOB": "MOBILE", "APP": "MOBILE",
    "TLR": "BRANCH", "TELLER": "BRANCH",
}


# ---------------------------------------------------------------------------
# Cleansing pipeline
# ---------------------------------------------------------------------------

class TransactionSilverCleansing:
    """
    Medallion Silver layer transformation.

    Steps applied (in order):
      1. Deduplication on transaction_id (keep latest ingestion)
      2. Data type enforcement and null handling
      3. Channel normalisation
      4. Currency validation
      5. PII tokenisation (account_number → masked token)
      6. Risk pre-classification flags (SAR threshold, high-risk MCC)
      7. Upsert into Silver Delta table
    """

    def __init__(self, spark: SparkSession, processing_date: Optional[date] = None):
        self.spark           = spark
        self.processing_date = processing_date or date.today()
        self.storage         = config.storage
        self.risk            = config.risk

        self.source_table = self.storage.bronze_path("financial", "transactions")
        self.target_table = self.storage.silver_path("financial", "transactions_clean")

    # ------------------------------------------------------------------
    # Read
    # ------------------------------------------------------------------

    def read_bronze(self) -> DataFrame:
        """Read today's Bronze partition (incremental processing)."""
        log.info("Reading Bronze transactions for date: %s", self.processing_date)
        df = (
            self.spark.read.format("delta")
            .load(self.source_table)
            .filter(F.col("transaction_date") == F.lit(str(self.processing_date)))
        )
        log.info("Bronze records read: %d", df.count())
        return df

    # ------------------------------------------------------------------
    # Deduplication
    # ------------------------------------------------------------------

    def deduplicate(self, df: DataFrame) -> DataFrame:
        """
        Deduplicate on transaction_id, keeping the most recently ingested row.
        CBS occasionally sends duplicate records on reruns or retries.
        """
        window = Window.partitionBy("transaction_id").orderBy(
            F.col("_ingestion_timestamp").desc()
        )
        deduped = (
            df.withColumn("_row_num", F.row_number().over(window))
              .filter(F.col("_row_num") == 1)
              .drop("_row_num")
        )
        dropped = df.count() - deduped.count()
        log.info("Deduplication — Removed %d duplicate records.", dropped)
        return deduped

    # ------------------------------------------------------------------
    # Standardise
    # ------------------------------------------------------------------

    def standardise(self, df: DataFrame) -> DataFrame:
        """Apply data type enforcement, null coalescing, and code normalisation."""

        # Build a broadcast-friendly channel map
        channel_map_expr = F.create_map(
            *[v for pair in CHANNEL_NORMALISATION.items() for v in
              (F.lit(pair[0]), F.lit(pair[1]))]
        )

        return (
            df
            # Uppercase text codes for consistency
            .withColumn("transaction_type",    F.upper(F.trim(F.col("transaction_type"))))
            .withColumn("transaction_channel", F.upper(F.trim(F.col("transaction_channel"))))
            .withColumn("currency_code",       F.upper(F.trim(F.col("currency_code"))))
            .withColumn("originating_country", F.upper(F.trim(F.col("originating_country"))))

            # Normalise channel codes using lookup map
            .withColumn(
                "transaction_channel",
                F.coalesce(
                    channel_map_expr[F.col("transaction_channel")],
                    F.col("transaction_channel")
                )
            )

            # Null coalescing for optional fields
            .withColumn("currency_code",        F.coalesce(F.col("currency_code"),       F.lit("USD")))
            .withColumn("originating_country",  F.coalesce(F.col("originating_country"), F.lit("US")))
            .withColumn("is_international",     F.coalesce(F.col("is_international"),    F.lit(False)))

            # Round monetary amounts to 2 decimal places
            .withColumn("amount_usd",          F.round(F.col("amount_usd"), 2))
            .withColumn("account_balance_usd", F.round(F.col("account_balance_usd"), 2))

            # Derive transaction hour for velocity analysis
            .withColumn("transaction_hour", F.hour(F.col("transaction_datetime")))
        )

    # ------------------------------------------------------------------
    # Currency validation
    # ------------------------------------------------------------------

    def validate_currency(self, df: DataFrame) -> DataFrame:
        """
        Flag transactions with unsupported currency codes.
        These are not rejected — they flow through with a flag for
        the FX compliance team.
        """
        accepted_set = F.array(*[F.lit(c) for c in ACCEPTED_CURRENCIES])
        return df.withColumn(
            "_flag_unsupported_currency",
            ~F.array_contains(accepted_set, F.col("currency_code"))
        )

    # ------------------------------------------------------------------
    # PII masking
    # ------------------------------------------------------------------

    def mask_pii(self, df: DataFrame) -> DataFrame:
        """
        Mask account_number for downstream consumers that don't require
        full account access.  A SHA-256 token is created for join purposes;
        the original account_number is retained in the restricted-access
        Silver table only — accessible only by credentialled pipelines.

        In production, the token key is rotated quarterly via Key Vault.
        """
        return df.withColumn(
            "account_number_token",
            F.sha2(F.concat(F.col("account_number"), F.lit("RISK_SALT_V3")), 256)
        )

    # ------------------------------------------------------------------
    # Risk classification
    # ------------------------------------------------------------------

    def apply_risk_flags(self, df: DataFrame) -> DataFrame:
        """
        Apply initial risk classification flags.

        These flags feed the Gold-layer credit risk scoring pipeline and
        the fraud monitoring dashboard.  They are NOT final determinations —
        they are signals for downstream analytical models.
        """
        high_risk_mcc = F.array(*[F.lit(m) for m in HIGH_RISK_MCC_CODES])

        return df.withColumns({
            # SAR threshold flag (FinCEN 31 CFR 1020.320)
            "_flag_sar_eligible": F.when(
                F.col("amount_usd") >= self.risk.sar_reporting_usd, F.lit(True)
            ).otherwise(F.lit(False)),

            # High-risk merchant category
            "_flag_high_risk_mcc": F.array_contains(
                high_risk_mcc, F.col("merchant_category_code")
            ),

            # Round-number transaction amounts (common in structuring patterns)
            "_flag_round_amount": F.when(
                (F.col("amount_usd") % 1000 == 0) &
                (F.col("amount_usd") >= 5_000), F.lit(True)
            ).otherwise(F.lit(False)),

            # Late-night high-value transactions (00:00–05:00 local)
            "_flag_off_hours_high_value": F.when(
                (F.col("transaction_hour").between(0, 5)) &
                (F.col("amount_usd") >= 2_000), F.lit(True)
            ).otherwise(F.lit(False)),

            # Silver processing metadata
            "_silver_processed_at":   F.current_timestamp(),
            "_silver_pipeline_ver":   F.lit("1.6.0"),
        })

    # ------------------------------------------------------------------
    # Write
    # ------------------------------------------------------------------

    def upsert(self, df: DataFrame) -> None:
        if DeltaTable.isDeltaTable(self.spark, self.target_table):
            log.info("Merging into existing Silver Delta table.")
            silver = DeltaTable.forPath(self.spark, self.target_table)
            (
                silver.alias("tgt")
                .merge(
                    df.alias("src"),
                    "tgt.transaction_id = src.transaction_id"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
        else:
            log.info("Creating Silver Delta table.")
            (
                df.write
                  .format("delta")
                  .mode("overwrite")
                  .partitionBy("transaction_date")
                  .option("delta.autoOptimize.optimizeWrite", "true")
                  .save(self.target_table)
            )
        log.info("Silver upsert complete.")

    # ------------------------------------------------------------------
    # Orchestrate
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info("=== Silver Cleansing | Date: %s ===", self.processing_date)
        try:
            df = self.read_bronze()
            df = self.deduplicate(df)
            df = self.standardise(df)
            df = self.validate_currency(df)
            df = self.mask_pii(df)
            df = self.apply_risk_flags(df)
            self.upsert(df)
            log.info("=== Silver cleansing completed successfully ===")
        except Exception as exc:
            log.error("Silver cleansing failed: %s", str(exc), exc_info=True)
            raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{config.spark.app_name}_SilverCleansing")
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled",   str(config.spark.adaptive_enabled).lower())
        .getOrCreate()
    )

    processing_date = spark.conf.get("pipeline.processing_date", str(date.today()))
    TransactionSilverCleansing(spark, date.fromisoformat(processing_date)).run()

