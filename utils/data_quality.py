"""
Data Quality Framework
-----------------------
Lightweight DQ checks for Silver and Gold Delta tables.
Raises pipeline alerts when thresholds are breached — prevents bad data
from reaching risk models and regulatory reports.

Checks are declarative: define a DQCheck, attach it to a table, run.
Results are written to a DQ audit log Delta table for trend monitoring.
"""

import logging
from dataclasses import dataclass, field
from datetime import date
from enum import Enum
from typing import List, Callable, Optional

from pyspark.sql import SparkSession, DataFrame
from pyspark.sql import functions as F

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Check definitions
# ---------------------------------------------------------------------------

class Severity(Enum):
    WARNING  = "WARNING"
    CRITICAL = "CRITICAL"


@dataclass
class DQCheck:
    """
    A single data quality assertion.

    name        : Human-readable identifier for reporting
    rule        : Lambda returning the count of FAILING records
    threshold   : Maximum allowed failure count (0 = no failures permitted)
    severity    : WARNING logs and continues; CRITICAL raises an exception
    description : Business context for the check (shown in audit log)
    """
    name: str
    rule: Callable[[DataFrame], int]
    threshold: int
    severity: Severity
    description: str


# ---------------------------------------------------------------------------
# Check catalogue for the Silver transactions_clean table
# ---------------------------------------------------------------------------

SILVER_TRANSACTION_CHECKS: List[DQCheck] = [
    DQCheck(
        name="null_transaction_id",
        rule=lambda df: df.filter(F.col("transaction_id").isNull()).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="transaction_id must never be null — primary key integrity."
    ),
    DQCheck(
        name="null_account_number",
        rule=lambda df: df.filter(F.col("account_number").isNull()).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="account_number is required for all risk aggregations."
    ),
    DQCheck(
        name="negative_amount",
        rule=lambda df: df.filter(F.col("amount_usd") < 0).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Transaction amounts must be non-negative after cleansing."
    ),
    DQCheck(
        name="invalid_transaction_type",
        rule=lambda df: df.filter(
            ~F.col("transaction_type").isin("CREDIT", "DEBIT", "TRANSFER", "FEE", "REVERSAL")
        ).count(),
        threshold=10,  # small tolerance for new CBS transaction codes
        severity=Severity.WARNING,
        description="Unexpected transaction type codes — may indicate CBS schema change."
    ),
    DQCheck(
        name="invalid_channel",
        rule=lambda df: df.filter(
            ~F.col("transaction_channel").isin("ONLINE", "MOBILE", "ATM", "BRANCH", "PHONE")
        ).count(),
        threshold=5,
        severity=Severity.WARNING,
        description="Channel values should be normalised by Silver cleansing."
    ),
    DQCheck(
        name="future_transaction_date",
        rule=lambda df: df.filter(
            F.col("transaction_date") > F.current_date()
        ).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Transaction dates in the future indicate a CBS system clock issue."
    ),
    DQCheck(
        name="missing_sar_flag",
        rule=lambda df: df.filter(
            (F.col("amount_usd") >= config.risk.sar_reporting_usd) &
            (F.col("_flag_sar_eligible") == False)
        ).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Every transaction >= $10,000 must carry the SAR eligibility flag."
    ),
    DQCheck(
        name="duplicate_transaction_id",
        rule=lambda df: df.count() - df.dropDuplicates(["transaction_id"]).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Silver layer must contain no duplicate transaction IDs."
    ),
]


# ---------------------------------------------------------------------------
# Check catalogue for the Gold credit_risk_features table
# ---------------------------------------------------------------------------

GOLD_CREDIT_RISK_CHECKS: List[DQCheck] = [
    DQCheck(
        name="null_account_features",
        rule=lambda df: df.filter(F.col("txn_count_30d").isNull()).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="30-day transaction count must be populated for all accounts."
    ),
    DQCheck(
        name="negative_feature_values",
        rule=lambda df: df.filter(
            (F.col("txn_count_7d") < 0) |
            (F.col("txn_total_amount_30d") < 0)
        ).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Aggregated feature values cannot be negative."
    ),
    DQCheck(
        name="online_ratio_out_of_range",
        rule=lambda df: df.filter(
            (F.col("online_txn_ratio_30d") < 0) |
            (F.col("online_txn_ratio_30d") > 1)
        ).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Online transaction ratio must be between 0 and 1."
    ),
    DQCheck(
        name="feature_date_consistency",
        rule=lambda df: df.filter(
            F.col("feature_date").isNull()
        ).count(),
        threshold=0,
        severity=Severity.CRITICAL,
        description="Every feature row must carry a feature_date for partitioning."
    ),
]


# ---------------------------------------------------------------------------
# DQ runner
# ---------------------------------------------------------------------------

@dataclass
class DQResult:
    table_path: str
    check_name: str
    description: str
    failing_count: int
    threshold: int
    severity: str
    passed: bool
    run_date: str = field(default_factory=lambda: str(date.today()))


class DataQualityRunner:

    def __init__(self, spark: SparkSession):
        self.spark      = spark
        self.audit_path = config.storage.gold_path("quality", "dq_audit_log")

    def run_checks(self, df: DataFrame, checks: List[DQCheck], table_path: str) -> List[DQResult]:
        results = []
        critical_failures = []

        for check in checks:
            try:
                failing_count = check.rule(df)
                passed        = failing_count <= check.threshold

                result = DQResult(
                    table_path=table_path,
                    check_name=check.name,
                    description=check.description,
                    failing_count=failing_count,
                    threshold=check.threshold,
                    severity=check.severity.value,
                    passed=passed,
                )
                results.append(result)

                if passed:
                    log.info("  ✓ %-40s  failing=%d / threshold=%d",
                             check.name, failing_count, check.threshold)
                else:
                    msg = (f"  ✗ {check.name} — "
                           f"failing={failing_count} > threshold={check.threshold}")
                    if check.severity == Severity.CRITICAL:
                        log.error(msg)
                        critical_failures.append(check.name)
                    else:
                        log.warning(msg)

            except Exception as exc:
                log.error("Check '%s' raised an exception: %s", check.name, str(exc))
                results.append(DQResult(
                    table_path=table_path,
                    check_name=check.name,
                    description=check.description,
                    failing_count=-1,
                    threshold=check.threshold,
                    severity=Severity.CRITICAL.value,
                    passed=False,
                ))
                critical_failures.append(check.name)

        self._write_audit_log(results)

        if critical_failures:
            raise ValueError(
                f"CRITICAL DQ failures on {table_path}: {critical_failures}. "
                f"Pipeline halted to protect downstream models and reports."
            )

        return results

    def _write_audit_log(self, results: List[DQResult]) -> None:
        rows = [
            (r.table_path, r.check_name, r.description,
             r.failing_count, r.threshold, r.severity,
             r.passed, r.run_date)
            for r in results
        ]
        schema = [
            "table_path", "check_name", "description",
            "failing_count", "threshold", "severity",
            "passed", "run_date"
        ]
        (
            self.spark.createDataFrame(rows, schema)
            .write.format("delta")
            .mode("append")
            .option("mergeSchema", "true")
            .save(self.audit_path)
        )
        log.info("DQ results written to audit log: %s", self.audit_path)

    def run_silver_checks(self, df: DataFrame) -> None:
        table = config.storage.silver_path("financial", "transactions_clean")
        log.info("=== Running Silver DQ Checks ===")
        self.run_checks(df, SILVER_TRANSACTION_CHECKS, table)

    def run_gold_credit_risk_checks(self, df: DataFrame) -> None:
        table = config.storage.gold_path("risk", "credit_risk_features")
        log.info("=== Running Gold Credit Risk Feature DQ Checks ===")
        self.run_checks(df, GOLD_CREDIT_RISK_CHECKS, table)

