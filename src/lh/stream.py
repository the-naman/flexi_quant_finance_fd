"""5.14 real-time payment events (FQF): Kafka -> Bronze -> Silver -> Gold with Structured Streaming.

Every run is a short availableNow run (read everything that has arrived, then stop); the state of each step lives in
its checkpoint, so nothing is read twice and a missed run is caught up by the next one.
  Bronze  bronze.payment_events          the Kafka message as it came (key, value text, topic, partition, offset, time)
  Silver  silver.payment_events          parsed events; broken messages left out; duplicates (same event_id within the
                                         watermark) removed; events later than the watermark dropped
  Gold    gold.payment_failure_windows   failed payments per customer per 10-minute window (written when the window is
                                         closed by the watermark), is_alert = failed_payments >= alert_failed
Events carry synthetic references only (customer_ref, loan_ref), never personal values. Rules: config/stream.yml.
"""
import os
import yaml
from pyspark.sql import functions as F, types as T
from lh import common

EVENT = T.StructType([
    T.StructField("event_id", T.StringType()), T.StructField("event_ts", T.TimestampType()),
    T.StructField("customer_ref", T.StringType()), T.StructField("loan_ref", T.StringType()),
    T.StructField("amount_paise", T.LongType()), T.StructField("status", T.StringType()),
    T.StructField("method", T.StringType()), T.StructField("reason", T.StringType())])


def load():
    with open(os.path.join(common.repo_root(), "config", "stream.yml")) as f:
        return yaml.safe_load(f)


def kafka_options(scfg, env, secret):
    """Spark Kafka source options; secret(key) reads the scope lh_<env>. Never printed."""
    jaas = ("kafkashaded.org.apache.kafka.common.security.scram.ScramLoginModule required "
            f'username="{scfg["consumer_user"].format(env=env)}" password="{secret("kafka_password")}";')
    return {"kafka.bootstrap.servers": secret("kafka_bootstrap"),
            "subscribe": scfg["topic"].format(env=env),
            "startingOffsets": "earliest", "failOnDataLoss": "false",
            "maxOffsetsPerTrigger": str(scfg["max_offsets_per_run"]),
            "kafka.security.protocol": "SASL_SSL", "kafka.sasl.mechanism": "SCRAM-SHA-256",
            "kafka.sasl.jaas.config": jaas,
            "kafka.ssl.truststore.type": "PEM", "kafka.ssl.truststore.certificates": secret("kafka_ca_cert")}


def to_bronze(kafka_df):
    """Kafka rows -> Bronze columns (value kept as text, nothing parsed yet)."""
    return kafka_df.select(F.col("key").cast("string").alias("kafka_key"), F.col("value").cast("string").alias("value"),
                           "topic", "partition", "offset", F.col("timestamp").alias("kafka_ts"),
                           F.current_timestamp().alias("_ingest_ts"))


def parse(bronze_df):
    """Bronze -> event columns; a message that is not a valid event gets event_id/event_ts NULL."""
    e = F.from_json("value", EVENT)
    return bronze_df.select(e.alias("e"), "topic", "partition", "offset", "kafka_ts").select(
        "e.*", "topic", "partition", "offset", "kafka_ts")


def to_silver(bronze_stream, scfg):
    """Valid events only, de-duplicated by event_id within the watermark; late events are dropped."""
    ok = (F.col("event_id").isNotNull() & F.col("event_ts").isNotNull() & F.col("customer_ref").isNotNull()
          & F.col("status").isin("success", "failed") & (F.col("amount_paise") >= 0))
    return (parse(bronze_stream).filter(ok)
            .withColumn("amount", (F.col("amount_paise") / 100).cast("decimal(18,2)")).drop("amount_paise")
            .withWatermark("event_ts", scfg["watermark"])
            .dropDuplicatesWithinWatermark(["event_id"])
            .withColumn("_silver_ts", F.current_timestamp()))


def to_gold(silver_stream, scfg):
    """Failed payments per customer per window; a row is written once its window is closed by the watermark.
    Every event (success too) is aggregated so that it moves event time on; filtering failures first would let Spark
    push the filter below the watermark, and quiet periods without failures would never close a window."""
    failed = F.col("status") == "failed"
    return (silver_stream.withWatermark("event_ts", scfg["watermark"])
            .groupBy(F.window("event_ts", scfg["window"]).alias("w"), "customer_ref")
            .agg(F.sum(failed.cast("int")).alias("failed_payments"),
                 F.sum(F.when(failed, F.col("amount"))).alias("failed_amount"),
                 F.min(F.when(failed, F.col("event_ts"))).alias("first_failed_ts"),
                 F.max(F.when(failed, F.col("event_ts"))).alias("last_failed_ts"))
            .filter("failed_payments > 0")
            .select(F.col("w.start").alias("window_start"), F.col("w.end").alias("window_end"), "customer_ref",
                    F.col("failed_payments").cast("int").alias("failed_payments"), "failed_amount",
                    "first_failed_ts", "last_failed_ts",
                    (F.col("failed_payments") >= scfg["alert_failed"]).alias("is_alert"),
                    F.current_timestamp().alias("_gold_ts")))


TABLES = {"bronze": "bronze.payment_events", "silver": "silver.payment_events", "gold": "gold.payment_failure_windows"}


def run(spark, cfg, layer, scfg, secret=None):
    """One availableNow run of one layer. Returns the number of rows read."""
    cat, env = cfg["catalog"], cfg["env"]
    if layer == "bronze":
        src = spark.readStream.format("kafka").options(**kafka_options(scfg, env, secret)).load()
        out = to_bronze(src)
    elif layer == "silver":
        out = to_silver(spark.readStream.table(f"{cat}.{TABLES['bronze']}"), scfg)
    elif layer == "gold":
        out = to_gold(spark.readStream.table(f"{cat}.{TABLES['silver']}"), scfg)
    else:
        raise ValueError(f"unknown layer {layer}")
    q = (out.writeStream.format("delta").outputMode("append")
         .option("checkpointLocation", f"/Volumes/{cat}/ops/checkpoints/stream/{layer}")
         .trigger(availableNow=True).toTable(f"{cat}.{TABLES[layer]}"))
    q.awaitTermination()
    rows = 0
    for p in q.recentProgress:              # dicts (classic) or progress objects (serverless / Spark Connect)
        n = p.get("numInputRows") if isinstance(p, dict) else getattr(p, "numInputRows", None)
        rows += int(n or 0)
    return rows
