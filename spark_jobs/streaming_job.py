"""
Speed Layer — Spark Structured Streaming
==========================================
Consumes `gps_events` from Kafka and, for every micro-batch:

  1. Archives raw events as Parquet, partitioned by date, to
     /data/parquet_archive/gps_events/  (this is the Lambda "immutable log /
     master dataset" that the BATCH layer replays from — it plays the role
     an S3/HDFS data lake would in production).
  2. Computes a 1-minute tumbling-window aggregate per zone (active
     vehicles, idle ratio, trips started, earnings) and upserts it into
     Postgres `vehicle_realtime_metrics`.
  3. Maintains the latest known state of every vehicle in
     `vehicle_current_state`, which is what drives the idle-vehicle alert.
  4. Writes a heartbeat row so the watchdog can detect pipeline staleness.

Run with:
  spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,\
      org.postgresql:postgresql:42.7.3 streaming_job.py
"""
import os
import sys

from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import (StructType, StructField, StringType,
                                DoubleType, BooleanType, TimestampType)

sys.path.append("/app")
from common.logging_utils import get_logger

logger = get_logger("spark_streaming_job")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("GPS_TOPIC", "gps_events")
PG_URL = os.environ.get("PG_JDBC_URL", "jdbc:postgresql://postgres:5432/fleetdb")
PG_USER = os.environ.get("POSTGRES_USER", "fleet")
PG_PASSWORD = os.environ.get("POSTGRES_PASSWORD", "fleetpass")
PARQUET_ARCHIVE_PATH = os.environ.get("PARQUET_ARCHIVE_PATH", "/data/parquet_archive/gps_events")
CHECKPOINT_BASE = os.environ.get("CHECKPOINT_DIR", "/data/checkpoints")
IDLE_ALERT_THRESHOLD_MIN = int(os.environ.get("IDLE_ALERT_THRESHOLD_MIN", "10"))

EVENT_SCHEMA = StructType([
    StructField("trip_id", StringType(), True),
    StructField("driver_id", StringType(), True),
    StructField("vehicle_id", StringType(), True),
    StructField("zone", StringType(), True),
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
    StructField("speed", DoubleType(), True),
    StructField("status", StringType(), True),
    StructField("fare", DoubleType(), True),
    StructField("trip_started", BooleanType(), True),
    StructField("timestamp", StringType(), True),
])


def get_spark() -> SparkSession:
    return (
        SparkSession.builder
        .appName("FleetSpeedLayer")
        .config("spark.sql.shuffle.partitions", "4")
        .getOrCreate()
    )


def write_postgres(df, table: str, mode: str = "append"):
    (df.write
       .format("jdbc")
       .option("url", PG_URL)
       .option("dbtable", table)
       .option("user", PG_USER)
       .option("password", PG_PASSWORD)
       .option("driver", "org.postgresql.Driver")
       .mode(mode)
       .save())


def upsert_vehicle_state(df, spark):
    """Upsert-by-vehicle_id via a temp staging table + SQL MERGE-like delete/insert,
    since plain JDBC append can't express upserts."""
    if df.rdd.isEmpty():
        return
    import psycopg2
    rows = df.collect()
    conn = psycopg2.connect(
        host=PG_URL.split("//")[1].split(":")[0].split("/")[0],
        dbname="fleetdb", user=PG_USER, password=PG_PASSWORD,
        port=5432,
    )
    cur = conn.cursor()
    for r in rows:
        cur.execute("""
            INSERT INTO vehicle_current_state
                (vehicle_id, driver_id, status, zone, lat, lon, speed, last_event_time, status_since, updated_at)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s, now(), now())
            ON CONFLICT (vehicle_id) DO UPDATE SET
                driver_id = EXCLUDED.driver_id,
                status_since = CASE WHEN vehicle_current_state.status IS DISTINCT FROM EXCLUDED.status
                                     THEN now() ELSE vehicle_current_state.status_since END,
                status = EXCLUDED.status,
                zone = EXCLUDED.zone,
                lat = EXCLUDED.lat,
                lon = EXCLUDED.lon,
                speed = EXCLUDED.speed,
                last_event_time = EXCLUDED.last_event_time,
                updated_at = now();
        """, (r.vehicle_id, r.driver_id, r.status, r.zone, r.lat, r.lon, r.speed, r.event_time))
    conn.commit()

    # ---- Idle-vehicle alert rule: flag vehicles idle beyond threshold ----
    cur.execute("""
        INSERT INTO alerts (alert_type, severity, subject_id, message)
        SELECT 'VEHICLE_IDLE', 'WARNING', vehicle_id,
               'Vehicle ' || vehicle_id || ' has been idle for over %s minutes'
        FROM vehicle_current_state
        WHERE status = 'idle'
          AND status_since < now() - interval '%s minutes'
          AND NOT EXISTS (
              SELECT 1 FROM alerts
              WHERE alert_type = 'VEHICLE_IDLE' AND resolved = false
                AND subject_id = vehicle_current_state.vehicle_id
          );
    """ % (IDLE_ALERT_THRESHOLD_MIN, IDLE_ALERT_THRESHOLD_MIN))
    conn.commit()

    # auto-resolve idle alerts for vehicles that are active again
    cur.execute("""
        UPDATE alerts SET resolved = true
        WHERE alert_type = 'VEHICLE_IDLE' AND resolved = false
          AND subject_id IN (SELECT vehicle_id FROM vehicle_current_state WHERE status <> 'idle');
    """)
    conn.commit()

    cur.execute("""
        INSERT INTO pipeline_heartbeat (component, last_heartbeat, events_processed_total)
        VALUES ('spark_streaming_job', now(), %s)
        ON CONFLICT (component) DO UPDATE SET
            last_heartbeat = now(),
            events_processed_total = pipeline_heartbeat.events_processed_total + %s;
    """, (len(rows), len(rows)))
    conn.commit()
    cur.close()
    conn.close()


def compute_zone_metrics(batch_df):
    """1-minute tumbling-window metrics per zone.

    Vehicle COUNTS use each vehicle's LAST known status/zone inside the window,
    so a vehicle is counted exactly once per window (active + idle == vehicles
    seen). Counting distinct vehicles per status instead would count a vehicle
    once for every status it passed through and inflate the totals.
    EARNINGS / TRIPS / SPEED are true window aggregates over all events.
    """
    from pyspark.sql.window import Window

    tagged = batch_df.withColumn("win", F.window("event_time", "1 minute"))

    latest_first = Window.partitionBy("win", "vehicle_id").orderBy(F.col("event_time").desc())
    last_state = (tagged.withColumn("rn", F.row_number().over(latest_first))
                        .filter(F.col("rn") == 1))
    status_counts = last_state.groupBy("win", "zone").agg(
        F.count(F.when(F.col("status") != "idle", 1)).alias("active_vehicles"),
        F.count(F.when(F.col("status") == "idle", 1)).alias("idle_vehicles"),
        F.count(F.when(F.col("status") == "enroute", 1)).alias("enroute_vehicles"),
        F.count(F.when(F.col("status") == "on_trip", 1)).alias("on_trip_vehicles"),
    )

    activity = tagged.groupBy("win", "zone").agg(
        F.sum(F.when(F.col("trip_started") == True, 1).otherwise(0)).alias("trips_started"),
        F.sum("fare").alias("total_earnings"),
        F.avg("speed").alias("avg_speed"),
    )

    joined = activity.join(status_counts, ["win", "zone"], "full_outer")
    active = F.coalesce("active_vehicles", F.lit(0))
    idle = F.coalesce("idle_vehicles", F.lit(0))
    return joined.select(
        F.col("win.start").alias("window_start"),
        F.col("win.end").alias("window_end"),
        "zone",
        active.alias("active_vehicles"),
        idle.alias("idle_vehicles"),
        F.coalesce("enroute_vehicles", F.lit(0)).alias("enroute_vehicles"),
        F.coalesce("on_trip_vehicles", F.lit(0)).alias("on_trip_vehicles"),
        (idle / F.greatest(active + idle, F.lit(1))).alias("idle_ratio"),
        F.coalesce("trips_started", F.lit(0)).alias("trips_started"),
        F.coalesce("total_earnings", F.lit(0.0)).alias("total_earnings"),
        "avg_speed",
    )


def process_batch(batch_df, batch_id: int):
    if batch_df.rdd.isEmpty():
        logger.info(f"batch_id={batch_id} empty, skipping")
        return
    batch_df.persist()
    count = batch_df.count()
    logger.info(f"batch_id={batch_id} processing {count} events")

    # 1. Archive raw events to Parquet (Lambda master dataset / replay log)
    archive_df = batch_df.withColumn("archive_date", F.to_date("event_time"))
    (archive_df.write.mode("append").partitionBy("archive_date").parquet(PARQUET_ARCHIVE_PATH))

    # 2. 1-minute windowed zone aggregation -> vehicle_realtime_metrics
    agg = compute_zone_metrics(batch_df)
    if agg.count() > 0:
        write_postgres(agg, "vehicle_realtime_metrics")

    # 3. Latest per-vehicle state -> vehicle_current_state (+ idle alerts + heartbeat)
    latest_state = (
        batch_df
        .withColumn("rn", F.row_number().over(
            __import__("pyspark.sql.window", fromlist=["Window"]).Window
            .partitionBy("vehicle_id").orderBy(F.col("event_time").desc())))
        .filter(F.col("rn") == 1)
        .select("vehicle_id", "driver_id", "status", "zone", "lat", "lon", "speed", "event_time")
    )
    upsert_vehicle_state(latest_state, None)

    batch_df.unpersist()


def main():
    spark = get_spark()
    spark.sparkContext.setLogLevel("WARN")
    logger.info(f"Reading from Kafka topic '{TOPIC}' at {KAFKA_BOOTSTRAP}")

    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP)
        .option("subscribe", TOPIC)
        .option("startingOffsets", "latest")
        .option("failOnDataLoss", "false")
        .load()
    )

    parsed = (
        raw.selectExpr("CAST(value AS STRING) as json_str")
        .select(F.from_json("json_str", EVENT_SCHEMA).alias("data"))
        .select("data.*")
        .withColumn("event_time", F.to_timestamp("timestamp"))
    )

    query = (
        parsed.writeStream
        .foreachBatch(process_batch)
        .option("checkpointLocation", os.path.join(CHECKPOINT_BASE, "streaming_job"))
        .trigger(processingTime="20 seconds")
        .start()
    )
    logger.info("Streaming query started")
    query.awaitTermination()


if __name__ == "__main__":
    main()
