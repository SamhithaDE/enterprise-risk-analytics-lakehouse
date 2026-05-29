"""
Streaming Ingestion — Real-Time Transaction Event Consumer
-----------------------------------------------------------
Consumes transaction events from Azure Event Hubs using Spark Structured
Streaming and writes micro-batches to the Bronze Delta table in near-real time.

This pipeline supports fraud monitoring use cases where batch latency (6+ hrs)
is insufficient.  Events arrive as JSON payloads from the transaction processing
system and are written to Bronze within seconds of origination.

Source  : Azure Event Hubs — transaction-events hub
Target  : Bronze Delta table — financial/transactions_stream
Trigger : Continuous streaming (Databricks always-on cluster)
"""

import logging
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, DoubleType, TimestampType, BooleanType, LongType
)

from config.pipeline_config import config

logging.basicConfig(level=logging.INFO, format="%(asctime)s [%(levelname)s] %(message)s")
log = logging.getLogger(__name__)


# ---------------------------------------------------------------------------
# Event payload schema
# Transaction events emitted by the real-time processing system.
# Subset of the full CBS schema — streaming events carry the fields needed
# for near-real-time fraud signal evaluation.
# ---------------------------------------------------------------------------

EVENT_PAYLOAD_SCHEMA = StructType([
    StructField("transaction_id",       StringType(),    nullable=False),
    StructField("account_number",       StringType(),    nullable=False),
    StructField("customer_id",          StringType(),    nullable=False),
    StructField("transaction_datetime", TimestampType(), nullable=False),
    StructField("transaction_type",     StringType(),    nullable=True),
    StructField("transaction_channel",  StringType(),    nullable=True),
    StructField("amount_usd",           DoubleType(),    nullable=False),
    StructField("currency_code",        StringType(),    nullable=True),
    StructField("originating_country",  StringType(),    nullable=True),
    StructField("is_international",     BooleanType(),   nullable=True),
    StructField("merchant_category_code", StringType(), nullable=True),
    StructField("merchant_name",        StringType(),    nullable=True),
    StructField("device_fingerprint",   StringType(),    nullable=True),   # mobile / online only
    StructField("ip_country",           StringType(),    nullable=True),   # online channel
    StructField("event_sequence_num",   LongType(),      nullable=True),
])


# ---------------------------------------------------------------------------
# Streaming pipeline
# ---------------------------------------------------------------------------

class TransactionStreamConsumer:
    """
    Structured Streaming pipeline:
      Event Hubs  →  parse JSON  →  enrich  →  Bronze Delta (append)

    Checkpointing ensures exactly-once delivery semantics.
    Watermarking handles late-arriving events (up to 10 minutes).
    """

    def __init__(self, spark: SparkSession):
        self.spark   = spark
        self.storage = config.storage
        self.eh      = config.event_hub

        self.target_table   = self.storage.bronze_path("financial", "transactions_stream")
        self.checkpoint_dir = (
            f"{self.storage.bronze_root.format(account=self.storage.storage_account)}"
            f"/_checkpoints/transactions_stream"
        )

    # ------------------------------------------------------------------
    # Build Event Hubs reader
    # ------------------------------------------------------------------

    def _build_source(self):
        """
        Construct the Event Hubs Structured Streaming source.
        Connection string is retrieved from Databricks secret scope — never
        embedded in code or config files.
        """
        # In a Databricks notebook: dbutils.secrets.get(scope, key)
        # Here we reference the secret path so the pattern is auditable.
        connection_string = self.eh.connection_string_secret

        eh_conf = {
            "eventhubs.connectionString": connection_string,
            "eventhubs.consumerGroup":    self.eh.consumer_group,
            "eventhubs.startingPosition": f'{{"offset":"{self.eh.starting_offsets}"}}',
            "maxEventsPerTrigger":        str(self.eh.max_events_per_trigger),
        }

        return (
            self.spark.readStream
            .format("eventhubs")
            .options(**eh_conf)
            .load()
        )

    # ------------------------------------------------------------------
    # Parse & enrich
    # ------------------------------------------------------------------

    def _parse_events(self, raw_df):
        """
        Decode Event Hubs body bytes → JSON → typed struct.
        Adds pipeline metadata and early fraud pre-screening columns.
        """
        parsed = (
            raw_df
            .withColumn("body_str", F.col("body").cast(StringType()))
            .withColumn("event",    F.from_json(F.col("body_str"), EVENT_PAYLOAD_SCHEMA))
            .select("event.*", "enqueuedTime", "sequenceNumber", "offset")
        )

        enriched = parsed.withColumns({
            # Pipeline metadata
            "_event_ingested_at": F.col("enqueuedTime"),
            "_event_sequence":    F.col("sequenceNumber"),
            "_pipeline":          F.lit("STREAM_EH_CONSUMER"),

            # Early fraud pre-screening signals
            # These are lightweight flags — full scoring happens in Gold layer
            "_flag_sar_threshold": F.when(
                F.col("amount_usd") >= config.risk.sar_reporting_usd, F.lit(True)
            ).otherwise(F.lit(False)),

            "_flag_international_high_value": F.when(
                (F.col("is_international") == True) &
                (F.col("amount_usd") >= 5_000.00),
                F.lit(True)
            ).otherwise(F.lit(False)),

            "_flag_ip_country_mismatch": F.when(
                (F.col("transaction_channel") == "ONLINE") &
                (F.col("ip_country") != F.col("originating_country")),
                F.lit(True)
            ).otherwise(F.lit(False)),
        })

        # Watermark for late-arriving events (handles up to 10-minute delays)
        return enriched.withWatermark("transaction_datetime", "10 minutes")

    # ------------------------------------------------------------------
    # Write (micro-batch)
    # ------------------------------------------------------------------

    def _write_stream(self, df):
        """
        Write each micro-batch to the Bronze streaming Delta table.
        Partitioned by ingestion date for efficient downstream reads.
        """
        df_with_partition = df.withColumn(
            "ingestion_date", F.to_date(F.col("_event_ingested_at"))
        )

        return (
            df_with_partition.writeStream
            .format("delta")
            .outputMode("append")
            .option("checkpointLocation", self.checkpoint_dir)
            .option("mergeSchema", "true")
            .partitionBy("ingestion_date")
            .trigger(processingTime=f"{config.delta.checkpoint_interval} seconds")
            .start(self.target_table)
        )

    # ------------------------------------------------------------------
    # Orchestrate
    # ------------------------------------------------------------------

    def run(self) -> None:
        log.info("=== Transaction Stream Consumer starting ===")
        log.info("Target table  : %s", self.target_table)
        log.info("Checkpoint dir: %s", self.checkpoint_dir)
        log.info("Event Hub     : %s / %s", self.eh.namespace, self.eh.hub_name)

        raw_df    = self._build_source()
        parsed_df = self._parse_events(raw_df)
        query     = self._write_stream(parsed_df)

        log.info("Streaming query active. Query ID: %s", query.id)
        query.awaitTermination()


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------

if __name__ == "__main__":
    spark = (
        SparkSession.builder
        .appName(f"{config.spark.app_name}_StreamConsumer")
        .config("spark.sql.shuffle.partitions",          config.spark.shuffle_partitions)
        .config("spark.sql.adaptive.enabled",            str(config.spark.adaptive_enabled).lower())
        .config("spark.sql.streaming.stateStore.providerClass",
                "com.databricks.sql.streaming.state.RocksDBStateStoreProvider")
        .getOrCreate()
    )

    TransactionStreamConsumer(spark).run()

