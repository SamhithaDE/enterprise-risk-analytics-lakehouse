"""
Gold Layer — Credit Risk Feature Engineering
--------------------------------------------
Builds ML-ready feature datasets for credit risk scoring models.

Features are computed at the account level using rolling windows over
Silver-layer transaction history.  Output is written to the Gold Delta
feature store and consumed by the Risk Models team for model training
and batch inference.

Source : Silver Delta — financial/transactions_clean
Target : Gold Delta  — risk/credit_risk_features
Refresh: Daily after Silver pipeline completes
"""

import logging
from datetime import date, timedelta
from typing import Optional

from pyspark.sql import SparkSession, DataFrame, Window
from pyspark.sql import functions as F
from delta.tables import DeltaTable

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


class CreditRiskFeatureEngineering:
    """
    Computes account-level behavioural features for credit risk modelling.

    Feature groups:
      - Transaction velocity     (count, volume over rolling windows)
      - Spending pattern         (channel mix, MCC concentration, avg ticket)
      - Balance behaviour        (utilisation trend, drawdown patterns)
      - Risk signal aggregation  (SAR flag counts, high-risk MCC exposure)
      - Temporal patterns        (weekend vs weekday, off-hours activity)
    """

    # Rolling window durations for feature computation
    WINDOWS = {
        "7d":  7,
        "30d": 30,
        "90d": 90,
    }

    def __init__(self, spark: SparkSession, feature_date: Optional[date] = None):
        self.spark        = spark
        self.feature_date = feature_date or date.today()
        self.storage      = config.storage

        self.source_table = self.storage.silver_path("financial", "transactions_clean")
        self.target_table = self.storage.gold_path("risk", "credit_risk_features")

        # Lookback horizon (max window + buffer for edge accounts)
        self.lookback_start = self.feature_date - timedelta(days=95)

    # ------------------------------------------------------------------
    # Read Silver data (scoped to lookback window)
    # ------------------------------------------------------------------

    def read_silver(self) -> DataFrame:
        log.info("Reading Silver transactions: %s → %s",
                 self.lookback_start, self.feature_date)

        df = (
            self.spark.read.format("delta")
            .load(self.source_table)
            .filter(
                (F.col("transaction_date") >= F.lit(str(self.lookback_start))) &
                (F.col("transaction_date") <= F.lit(str(self.feature_date)))
            )
        )
        log.info("Transactions in lookback window: %d", df.count())
        return df

    # ------------------------------------------------------------------
    # Helper: rolling window aggregation
    # ------------------------------------------------------------------

    def _build_rolling_features(self, df: DataFrame, days: int, suffix: str) -> DataFrame:
        """
        Compute per-account rolling aggregations over the last `days` calendar days
        relative to feature_date.

        Returns a DataFrame keyed on account_number with feature columns
        suffixed by `suffix` (e.g. _7d, _30d, _90d).
        """
        cutoff = self.feature_date - timedelta(days=days)
        window_df = df.filter(F.col("transaction_date") >= F.lit(str(cutoff)))

        features = (
            window_df.groupBy("account_number")
            .agg(
                # Volume & velocity
                F.count("transaction_id")                .alias(f"txn_count_{suffix}"),
                F.sum("amount_usd")                      .alias(f"txn_total_amount_{suffix}"),
                F.avg("amount_usd")                      .alias(f"txn_avg_amount_{suffix}"),
                F.max("amount_usd")                      .alias(f"txn_max_amount_{suffix}"),
                F.stddev("amount_usd")                   .alias(f"txn_stddev_amount_{suffix}"),

                # Channel mix
                F.sum(F.when(F.col("transaction_channel") == "ONLINE",  1).otherwise(0))
                 .alias(f"online_txn_count_{suffix}"),
                F.sum(F.when(F.col("transaction_channel") == "ATM",     1).otherwise(0))
                 .alias(f"atm_txn_count_{suffix}"),
                F.sum(F.when(F.col("transaction_channel") == "BRANCH",  1).otherwise(0))
                 .alias(f"branch_txn_count_{suffix}"),
                F.sum(F.when(F.col("transaction_channel") == "MOBILE",  1).otherwise(0))
                 .alias(f"mobile_txn_count_{suffix}"),

                # International exposure
                F.sum(F.when(F.col("is_international"), F.col("amount_usd")).otherwise(0))
                 .alias(f"intl_amount_{suffix}"),
                F.count(F.when(F.col("is_international"), True))
                 .alias(f"intl_txn_count_{suffix}"),

                # Risk signal aggregation
                F.sum(F.when(F.col("_flag_sar_eligible"),      1).otherwise(0))
                 .alias(f"sar_flag_count_{suffix}"),
                F.sum(F.when(F.col("_flag_high_risk_mcc"),     1).otherwise(0))
                 .alias(f"high_risk_mcc_count_{suffix}"),
                F.sum(F.when(F.col("_flag_round_amount"),      1).otherwise(0))
                 .alias(f"round_amount_count_{suffix}"),
                F.sum(F.when(F.col("_flag_off_hours_high_value"), 1).otherwise(0))
                 .alias(f"off_hours_count_{suffix}"),

                # Balance behaviour
                F.avg("account_balance_usd")             .alias(f"avg_balance_{suffix}"),
                F.min("account_balance_usd")             .alias(f"min_balance_{suffix}"),

                # Temporal patterns
                F.sum(F.when(F.dayofweek(F.col("transaction_date")).isin(1, 7), 1).otherwise(0))
                 .alias(f"weekend_txn_count_{suffix}"),
                F.countDistinct(F.col("transaction_date"))
                 .alias(f"active_days_{suffix}"),
            )
        )
        return features

    # ------------------------------------------------------------------
    # Compute features
    # ------------------------------------------------------------------

    def compute_features(self, df: DataFrame) -> DataFrame:
        """
        Join rolling window feature sets into a single wide feature table
        keyed on account_number.
        """
        log.info("Computing rolling window features: %s", list(self.WINDOWS.keys()))

        # Base: unique accounts in the lookback period
        accounts = df.select("account_number", "customer_id") \
                     .dropDuplicates(["account_number"])

        # Compute and join each rolling window
        feature_df = accounts
        for suffix, days in self.WINDOWS.items():
            window_features = self._build_rolling_features(df, days, suffix)
            feature_df = feature_df.join(window_features, on="account_number", how="left")

        # Derived ratio features (model interpretability)
        feature_df = feature_df.withColumns({
            # Online transaction share (30-day) — higher share correlates with
            # lower risk for retail customers, higher risk for business accounts
            "online_txn_ratio_30d": F.when(
                F.col("txn_count_30d") > 0,
                F.col("online_txn_count_30d") / F.col("txn_count_30d")
            ).otherwise(F.lit(0.0)),

            # International transaction share (90-day)
            "intl_txn_ratio_90d": F.when(
                F.col("txn_count_90d") > 0,
                F.col("intl_txn_count_90d") / F.col("txn_count_90d")
            ).otherwise(F.lit(0.0)),

            # Velocity ratio: 7-day vs 30-day (spike detection)
            "velocity_ratio_7d_vs_30d": F.when(
                F.col("txn_count_30d") > 0,
                (F.col("txn_count_7d") / 7.0) / (F.col("txn_count_30d") / 30.0)
            ).otherwise(F.lit(1.0)),

            # Average daily activity (30-day normalised)
            "avg_daily_txn_count_30d": F.when(
                F.col("active_days_30d") > 0,
                F.col("txn_count_30d") / F.col("active_days_30d")
            ).otherwise(F.lit(0.0)),
        })

        # Feature store metadata
        feature_df = feature_df.withColumns({
            "feature_date":        F.lit(str(self.feature_date)),
            "_feature_created_at": F.current_timestamp(),
            "_pipeline_version":   F.lit("3.1.0"),
        })

        log.info("Feature engineering complete. Columns: %d", len(feature_df.columns))
        return feature_df

    # ------------------------------------------------------------------
    # Write to Gold feature store
    # ------------------------------------------------------------------

    def write_features(self, df: DataFrame) -> None:
        if DeltaTable.isDeltaTable(self.spark, self.target_table):
            gold = DeltaTable.forPath(self.spark, self.target_table)
            (
                gold.alias("tgt")
                .merge(
                    df.alias("src"),
                    "tgt.account_number = src.account_number AND tgt.feature_date = src.feature_date"
                )
                .whenMatchedUpdateAll()
                .whenNotMatchedInsertAll()
                .execute()
            )
            log.info("Gold feature store updated via MERGE.")
        else:
            (
                df.write
                  .format("delta")
                  .mode("overwrite")
                  .partitionBy("feature_date")
                  .option("delta.autoOptimize.optimizeWrite", "true")
                  .save(self.target_table)
            )
            log.info("Gold feature store created.")

    # ------------------------------------------------------------------
    # Orchestrate
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info("=== Credit Risk Feature Engineering | Feature date: %s ===", self.feature_date)
        try:
            silver_df  = self.read_silver()
            feature_df = self.compute_features(silver_df)
            self.write_features(feature_df)
            log.info("=== Feature engineering completed successfully ===")
        except Exception as exc:
            log.error("Feature engineering failed: %s", str(exc), exc_info=True)
            raise


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{config.spark.app_name}_CreditRiskFeatures")
        .config("spark.sql.shuffle.partitions", config.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled",   str(config.spark.adaptive_enabled).lower())
        .getOrCreate()
    )

    feature_date = date.fromisoformat(
        spark.conf.get("pipeline.feature_date", str(date.today()))
    )
    CreditRiskFeatureEngineering(spark, feature_date).run()

