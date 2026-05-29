"""
Delta Lake Optimisation
-----------------------
Runs OPTIMIZE with Z-Ordering on Silver and Gold Delta tables to maximise
query performance for risk analytics workloads.

Why Z-Order on (account_number, transaction_date)?
  Risk and fraud analysts predominantly filter on account + date range.
  Z-Ordering co-locates files for that column combination, allowing Delta
  to skip 60–80% of files on typical reporting queries.

Scheduled: Daily after Gold pipeline completes (low-traffic window).
"""

import logging
from dataclasses import dataclass
from typing import List, Optional

from pyspark.sql import SparkSession
from delta.tables import DeltaTable

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


@dataclass
class TableOptimisationSpec:
    """Defines the optimisation strategy for a single Delta table."""
    path: str
    z_order_cols: List[str]
    vacuum_retain_hours: int = 720   # 30 days


# ---------------------------------------------------------------------------
# Tables to optimise (add new tables here as the lakehouse grows)
# ---------------------------------------------------------------------------

def get_optimisation_specs() -> List[TableOptimisationSpec]:
    s = config.storage
    return [
        # Silver
        TableOptimisationSpec(
            path=s.silver_path("financial", "transactions_clean"),
            z_order_cols=["account_number", "transaction_date"],
        ),
        # Gold — risk features
        TableOptimisationSpec(
            path=s.gold_path("risk", "credit_risk_features"),
            z_order_cols=["account_number", "feature_date"],
        ),
        # Gold — fraud signals
        TableOptimisationSpec(
            path=s.gold_path("risk", "fraud_signals"),
            z_order_cols=["account_number", "signal_date"],
            vacuum_retain_hours=168,   # 7 days (high churn table)
        ),
    ]


# ---------------------------------------------------------------------------
# Optimiser
# ---------------------------------------------------------------------------

class DeltaOptimiser:

    def __init__(self, spark: SparkSession, dry_run: bool = False):
        self.spark   = spark
        self.dry_run = dry_run

    def optimise_table(self, spec: TableOptimisationSpec) -> dict:
        """
        Run OPTIMIZE + ZORDER on a single Delta table.
        Returns a summary dict for the pipeline audit log.
        """
        log.info("Optimising: %s", spec.path)
        log.info("  Z-Order cols     : %s", spec.z_order_cols)
        log.info("  Vacuum retain hrs: %d", spec.vacuum_retain_hours)

        result = {"path": spec.path, "status": "skipped (dry_run)"}

        if not self.dry_run:
            try:
                # OPTIMIZE with Z-Ordering
                z_cols = ", ".join(spec.z_order_cols)
                self.spark.sql(f"""
                    OPTIMIZE delta.`{spec.path}`
                    ZORDER BY ({z_cols})
                """)
                log.info("  OPTIMIZE complete.")

                # Vacuum old snapshots (respects retention window)
                retain_hrs = spec.vacuum_retain_hours
                self.spark.sql(f"""
                    VACUUM delta.`{spec.path}`
                    RETAIN {retain_hrs} HOURS
                """)
                log.info("  VACUUM complete (retain %d hours).", retain_hrs)

                # Capture post-optimisation metrics
                detail = (
                    self.spark.sql(f"DESCRIBE DETAIL delta.`{spec.path}`")
                    .select("numFiles", "sizeInBytes", "lastModified")
                    .collect()[0]
                )
                result = {
                    "path":           spec.path,
                    "status":         "success",
                    "num_files":      detail["numFiles"],
                    "size_bytes":     detail["sizeInBytes"],
                    "last_modified":  str(detail["lastModified"]),
                }
                log.info("  Post-optimise: %d files | %.2f GB",
                         detail["numFiles"], detail["sizeInBytes"] / (1024**3))

            except Exception as exc:
                log.error("Optimisation failed for %s: %s", spec.path, str(exc))
                result = {"path": spec.path, "status": "failed", "error": str(exc)}

        return result

    def run(self) -> None:
        specs   = get_optimisation_specs()
        results = []

        log.info("=== Delta Optimisation Run | %d tables | dry_run=%s ===",
                 len(specs), self.dry_run)

        for spec in specs:
            result = self.optimise_table(spec)
            results.append(result)

        # Summary
        success = sum(1 for r in results if r.get("status") == "success")
        failed  = sum(1 for r in results if r.get("status") == "failed")
        log.info("=== Optimisation complete: %d success | %d failed ===", success, failed)

        if failed > 0:
            raise RuntimeError(f"{failed} table(s) failed optimisation — check logs.")


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{config.spark.app_name}_DeltaOptimiser")
        .config("spark.databricks.delta.retentionDurationCheck.enabled", "false")
        .getOrCreate()
    )

    dry_run = spark.conf.get("pipeline.dry_run", "false").lower() == "true"
    DeltaOptimiser(spark, dry_run=dry_run).run()

