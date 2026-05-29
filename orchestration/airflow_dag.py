"""
Airflow DAG — Risk Analytics Lakehouse Daily Pipeline
------------------------------------------------------
Orchestrates the full Bronze → Silver → Gold → Optimise sequence.

DAG ID     : risk_analytics_lakehouse_daily
Schedule   : Daily at 02:00 UTC (after CBS nightly export lands)
Owner      : Data Engineering — Risk Analytics Team
SLA        : Gold features available by 06:00 UTC for model inference jobs

Dependencies:
  - CBS nightly export must be present in landing zone (ExternalTaskSensor)
  - Databricks workspace connection configured in Airflow (conn_id: databricks_default)

Retry policy:
  - 2 retries with 5-minute delay for transient Databricks/ADLS failures
  - CRITICAL DQ failures do NOT retry (data issue — needs human review)
"""

from datetime import datetime, timedelta
from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator
from airflow.providers.databricks.sensors.databricks import DatabricksRunNowSensor
from airflow.operators.python import PythonOperator
from airflow.sensors.external_task import ExternalTaskSensor
from airflow.utils.trigger_rule import TriggerRule


# ---------------------------------------------------------------------------
# Defaults
# ---------------------------------------------------------------------------

DEFAULT_ARGS = {
    "owner":            "risk-data-engineering",
    "depends_on_past":  False,
    "start_date":       datetime(2025, 1, 1),
    "email_on_failure": True,
    "email":            ["data-eng-alerts@bank.internal"],
    "retries":          2,
    "retry_delay":      timedelta(minutes=5),
    "execution_timeout": timedelta(hours=2),
}

DATABRICKS_CONN   = "databricks_risk_workspace"
CLUSTER_POLICY_ID = "risk-analytics-policy"   # enforces instance type & autoscale limits

# Base Databricks notebook/script paths (relative to Databricks workspace root)
NOTEBOOK_BASE = "/Repos/risk-analytics/enterprise-risk-lakehouse"


# ---------------------------------------------------------------------------
# Cluster spec (shared across all tasks — autoscaling 4–20 workers)
# ---------------------------------------------------------------------------

def _cluster_spec(min_workers: int = 4, max_workers: int = 20) -> dict:
    return {
        "spark_version":  "14.3.x-scala2.12",
        "node_type_id":   "Standard_DS4_v2",
        "autoscale": {
            "min_workers": min_workers,
            "max_workers": max_workers,
        },
        "spark_conf": {
            "spark.sql.shuffle.partitions":             "400",
            "spark.sql.adaptive.enabled":               "true",
            "spark.databricks.delta.schema.autoMerge.enabled": "true",
        },
        "cluster_log_conf": {
            "dbfs": {"destination": "dbfs:/cluster-logs/risk-analytics"}
        },
    }


def _notebook_task(notebook_path: str, params: dict = None) -> dict:
    return {
        "notebook_task": {
            "notebook_path": notebook_path,
            "base_parameters": params or {},
        },
        "new_cluster": _cluster_spec(),
    }


def _python_task(script_path: str, params: dict = None) -> dict:
    return {
        "spark_python_task": {
            "python_file": f"dbfs:/pipelines/{script_path}",
            "parameters":  [f"--{k}={v}" for k, v in (params or {}).items()],
        },
        "new_cluster": _cluster_spec(),
    }


# ---------------------------------------------------------------------------
# DAG definition
# ---------------------------------------------------------------------------

with DAG(
    dag_id="risk_analytics_lakehouse_daily",
    default_args=DEFAULT_ARGS,
    schedule_interval="0 2 * * *",    # 02:00 UTC daily
    catchup=False,
    max_active_runs=1,
    tags=["risk", "lakehouse", "data-engineering"],
    doc_md="""
    ## Risk Analytics Lakehouse — Daily Pipeline

    Ingests CBS transaction exports, applies Silver cleansing and risk flags,
    builds Gold credit risk features, and optimises Delta tables for analyst queries.

    **SLA**: Gold features available by 06:00 UTC.
    **On failure**: Slack alert → #risk-data-eng-alerts + email to team DL.
    """,
) as dag:

    # ------------------------------------------------------------------
    # Wait for CBS landing zone file (upstream dependency)
    # ------------------------------------------------------------------
    wait_for_cbs_export = ExternalTaskSensor(
        task_id="wait_for_cbs_export",
        external_dag_id="cbs_nightly_export",
        external_task_id="export_complete",
        timeout=3600,
        poke_interval=120,
        mode="reschedule",
    )

    # ------------------------------------------------------------------
    # Bronze ingestion
    # ------------------------------------------------------------------
    bronze_transaction_ingestion = DatabricksSubmitRunOperator(
        task_id="bronze_transaction_ingestion",
        conn_id=DATABRICKS_CONN,
        json=_python_task(
            "ingestion/bronze/transaction_ingestion.py",
            {"pipeline.ingestion_date": "{{ ds }}"}
        ),
    )

    # ------------------------------------------------------------------
    # Silver cleansing
    # ------------------------------------------------------------------
    silver_transaction_cleansing = DatabricksSubmitRunOperator(
        task_id="silver_transaction_cleansing",
        conn_id=DATABRICKS_CONN,
        json=_python_task(
            "transformation/silver/transaction_cleansing.py",
            {"pipeline.processing_date": "{{ ds }}"}
        ),
    )

    # ------------------------------------------------------------------
    # Silver DQ gate (fail-fast before Gold)
    # ------------------------------------------------------------------
    silver_dq_check = DatabricksSubmitRunOperator(
        task_id="silver_dq_check",
        conn_id=DATABRICKS_CONN,
        json=_notebook_task(
            f"{NOTEBOOK_BASE}/notebooks/dq/silver_transaction_dq",
            {"processing_date": "{{ ds }}"}
        ),
        retries=0,   # DQ failures should NOT be retried automatically
    )

    # ------------------------------------------------------------------
    # Gold — Credit risk features
    # ------------------------------------------------------------------
    gold_credit_risk_features = DatabricksSubmitRunOperator(
        task_id="gold_credit_risk_features",
        conn_id=DATABRICKS_CONN,
        json=_python_task(
            "transformation/gold/credit_risk_features.py",
            {"pipeline.feature_date": "{{ ds }}"}
        ),
    )

    # ------------------------------------------------------------------
    # Gold — Fraud signal pipeline (runs in parallel with credit risk)
    # ------------------------------------------------------------------
    gold_fraud_signals = DatabricksSubmitRunOperator(
        task_id="gold_fraud_signals",
        conn_id=DATABRICKS_CONN,
        json=_python_task(
            "transformation/gold/fraud_signal_pipeline.py",
            {"pipeline.signal_date": "{{ ds }}"}
        ),
    )

    # ------------------------------------------------------------------
    # Gold DQ gate
    # ------------------------------------------------------------------
    gold_dq_check = DatabricksSubmitRunOperator(
        task_id="gold_dq_check",
        conn_id=DATABRICKS_CONN,
        json=_notebook_task(
            f"{NOTEBOOK_BASE}/notebooks/dq/gold_credit_risk_dq",
            {"feature_date": "{{ ds }}"}
        ),
        retries=0,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Delta optimisation (runs after both Gold tasks succeed)
    # ------------------------------------------------------------------
    delta_optimise = DatabricksSubmitRunOperator(
        task_id="delta_optimise",
        conn_id=DATABRICKS_CONN,
        json=_python_task(
            "utils/delta_optimizer.py",
            {"pipeline.dry_run": "false"}
        ),
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ------------------------------------------------------------------
    # Pipeline success notification
    # ------------------------------------------------------------------
    def _notify_success(**context):
        run_date = context["ds"]
        print(f"[SUCCESS] Risk Analytics Lakehouse pipeline completed for {run_date}.")
        # In production: post to Slack #risk-data-eng via webhook

    notify_success = PythonOperator(
        task_id="notify_success",
        python_callable=_notify_success,
        trigger_rule=TriggerRule.ALL_SUCCESS,
    )

    # ------------------------------------------------------------------
    # DAG dependency graph
    #
    #   wait_for_cbs_export
    #         │
    #   bronze_ingestion
    #         │
    #   silver_cleansing
    #         │
    #   silver_dq_check
    #       ┌─┴────────────────┐
    #  gold_credit_risk    gold_fraud
    #       └─────────┬────────┘
    #          gold_dq_check
    #                │
    #          delta_optimise
    #                │
    #          notify_success
    # ------------------------------------------------------------------

    (
        wait_for_cbs_export
        >> bronze_transaction_ingestion
        >> silver_transaction_cleansing
        >> silver_dq_check
        >> [gold_credit_risk_features, gold_fraud_signals]
        >> gold_dq_check
        >> delta_optimise
        >> notify_success
    )

