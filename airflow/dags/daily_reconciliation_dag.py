"""
Batch Layer — Daily Profitability Reconciliation DAG
=======================================================
This DAG is the BATCH LAYER of the Lambda architecture. Once a (simulated)
day, it:

  1. Waits for / detects the day's vehicle-cost CSV dropped by the batch
     producer (sensor step models "file arrival").
  2. Reads the Parquet trip archive (the master dataset written by the
     Spark speed-layer job) and aggregates trips/earnings per vehicle for
     that business date — this re-derives batch views straight from the
     immutable log, the core Lambda batch-layer property.
  3. Joins that with the day's fuel/maintenance costs.
  4. Computes net profit and flags unprofitable vehicles.
  5. Writes the merged result to `vehicle_profitability_report` (the
     Lambda serving-layer merge of speed-layer-derived trip data and
     batch-layer cost data) and raises an alert for any vehicle whose
     margin is negative.
  6. Runs a data-freshness health check as its own task, independent of
     whether the reconciliation itself succeeds, so pipeline staleness is
     always observable.

Schedule: every SIMULATED_DAY_SECONDS (default matches batch_producer's
5-minute simulated day). In production this would simply be `@daily`.
"""
import glob
import os
from datetime import datetime, timedelta

import pandas as pd
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor

import sys
sys.path.append("/opt/airflow")
from common.logging_utils import get_logger, log_with_fields

logger = get_logger("airflow_daily_reconciliation")

BATCH_INCOMING_DIR = "/data/batch_incoming"
PARQUET_ARCHIVE_PATH = "/data/parquet_archive/gps_events"
SIMULATED_DAY_SECONDS = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))

PG_CONN = {
    "host": "postgres", "dbname": "fleetdb",
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleetpass"),
    "port": 5432,
}

default_args = {
    "owner": "fleet-data-eng",
    "retries": 2,
    "retry_delay": timedelta(seconds=30),
}


def _business_date_for_run(execution_date) -> str:
    return execution_date.strftime("%Y-%m-%d")


def find_latest_cost_file(**context):
    files = sorted(glob.glob(os.path.join(BATCH_INCOMING_DIR, "vehicle_costs_*.csv")))
    if not files:
        raise FileNotFoundError("No vehicle_costs_*.csv files found yet")
    latest = files[-1]
    business_date = os.path.basename(latest).replace("vehicle_costs_", "").replace(".csv", "")
    context["ti"].xcom_push(key="cost_file_path", value=latest)
    context["ti"].xcom_push(key="business_date", value=business_date)
    log_with_fields(logger, "info", "cost_file_located", path=latest, business_date=business_date)


def reconcile_and_load(**context):
    import psycopg2

    ti = context["ti"]
    cost_file_path = ti.xcom_pull(key="cost_file_path", task_ids="find_latest_cost_file")
    business_date = ti.xcom_pull(key="business_date", task_ids="find_latest_cost_file")

    costs = pd.read_csv(cost_file_path)
    costs["vehicle_id"] = costs["vehicle_id"].astype(str)

    # Re-derive trip aggregates for that day directly from the Parquet
    # archive (the immutable event log) -- this is the batch layer doing
    # its own independent recomputation, per Lambda architecture.
    trip_agg = pd.DataFrame(columns=["vehicle_id", "total_trips", "total_earnings"])
    # SIMULATED-CLOCK NOTE: one simulated day == SIMULATED_DAY_SECONDS of real
    # time, so "that day's trips" = archived events from the last
    # SIMULATED_DAY_SECONDS of wall-clock time (NOT events whose real calendar
    # date equals the cost file's simulated business_date, which advances
    # faster than real dates and would never match).
    parquet_files = glob.glob(os.path.join(PARQUET_ARCHIVE_PATH, "archive_date=*", "*.parquet"))
    if parquet_files:
        frames = [pd.read_parquet(f) for f in parquet_files]
        events = pd.concat(frames, ignore_index=True)
        events["event_time"] = pd.to_datetime(events["event_time"], utc=True)
        cutoff = pd.Timestamp.now(tz="UTC") - pd.Timedelta(seconds=SIMULATED_DAY_SECONDS)
        events = events[events["event_time"] >= cutoff]
        on_trip = events[events["status"] == "on_trip"].copy()
        trip_agg = (
            on_trip.groupby("vehicle_id")
            .agg(total_trips=("trip_id", "nunique"), total_earnings=("fare", "sum"))
            .reset_index()
        )
    else:
        log_with_fields(logger, "warning", "no_parquet_archive_for_date", business_date=business_date)

    merged = costs.merge(trip_agg, on="vehicle_id", how="left")
    merged["total_trips"] = merged["total_trips"].fillna(0).astype(int)
    merged["total_earnings"] = merged["total_earnings"].fillna(0.0)
    merged["net_profit"] = merged["total_earnings"] - merged["fuel_cost"] - merged["maintenance_cost"]
    merged["profit_margin_pct"] = merged.apply(
        lambda r: (r["net_profit"] / r["total_earnings"] * 100) if r["total_earnings"] > 0 else -100.0,
        axis=1,
    )
    merged["is_unprofitable"] = merged["net_profit"] < 0

    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    for _, r in merged.iterrows():
        cur.execute("""
            INSERT INTO vehicle_profitability_report
                (business_date, vehicle_id, total_trips, total_earnings, fuel_cost,
                 maintenance_cost, net_profit, profit_margin_pct, is_unprofitable)
            VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s)
            ON CONFLICT (business_date, vehicle_id) DO UPDATE SET
                total_trips = EXCLUDED.total_trips,
                total_earnings = EXCLUDED.total_earnings,
                fuel_cost = EXCLUDED.fuel_cost,
                maintenance_cost = EXCLUDED.maintenance_cost,
                net_profit = EXCLUDED.net_profit,
                profit_margin_pct = EXCLUDED.profit_margin_pct,
                is_unprofitable = EXCLUDED.is_unprofitable,
                generated_at = now();
        """, (business_date, r.vehicle_id, int(r.total_trips), float(r.total_earnings),
              float(r.fuel_cost), float(r.maintenance_cost), float(r.net_profit),
              float(r.profit_margin_pct), bool(r.is_unprofitable)))

        if r.is_unprofitable:
            cur.execute("""
                INSERT INTO alerts (alert_type, severity, subject_id, message)
                VALUES ('UNPROFITABLE_VEHICLE', 'WARNING', %s, %s)
            """, (r.vehicle_id,
                  f"Vehicle {r.vehicle_id} is unprofitable on {business_date}: "
                  f"net_profit={r.net_profit:.2f}"))

    cur.execute("""
        INSERT INTO pipeline_heartbeat (component, last_heartbeat, events_processed_total)
        VALUES ('airflow_daily_reconciliation', now(), %s)
        ON CONFLICT (component) DO UPDATE SET
            last_heartbeat = now(),
            events_processed_total = pipeline_heartbeat.events_processed_total + %s;
    """, (len(merged), len(merged)))
    conn.commit()
    cur.close()
    conn.close()

    log_with_fields(logger, "info", "reconciliation_complete",
                     business_date=business_date, vehicles=len(merged),
                     unprofitable=int(merged["is_unprofitable"].sum()))


def freshness_health_check(**context):
    """Independent observability task: flags PIPELINE_STALE if the batch
    source hasn't dropped a new file in over 2x the expected interval."""
    import psycopg2
    files = sorted(glob.glob(os.path.join(BATCH_INCOMING_DIR, "vehicle_costs_*.csv")))
    conn = psycopg2.connect(**PG_CONN)
    cur = conn.cursor()
    if not files:
        cur.execute("""INSERT INTO alerts (alert_type, severity, subject_id, message)
                        VALUES ('PIPELINE_STALE', 'CRITICAL', 'batch_producer',
                                'No vehicle_costs files found at all')""")
        conn.commit()
        cur.close(); conn.close()
        return
    latest_mtime = max(os.path.getmtime(f) for f in files)
    age_seconds = datetime.now().timestamp() - latest_mtime
    if age_seconds > 2 * SIMULATED_DAY_SECONDS:
        cur.execute("""INSERT INTO alerts (alert_type, severity, subject_id, message)
                        VALUES ('PIPELINE_STALE', 'CRITICAL', 'batch_producer', %s)""",
                    (f"No new daily cost file for {age_seconds:.0f}s "
                     f"(expected every {SIMULATED_DAY_SECONDS}s)",))
        conn.commit()
    cur.close()
    conn.close()


with DAG(
    dag_id="daily_fleet_reconciliation",
    description="Batch layer: join daily vehicle costs with the Parquet trip archive "
                 "to produce the per-vehicle profitability report (Lambda architecture).",
    default_args=default_args,
    schedule_interval=timedelta(seconds=SIMULATED_DAY_SECONDS),
    start_date=datetime(2026, 1, 1),
    catchup=False,
    tags=["batch-layer", "lambda-architecture", "fleet"],
) as dag:

    wait_for_cost_file = FileSensor(
        task_id="wait_for_cost_file",
        filepath=BATCH_INCOMING_DIR,
        fs_conn_id="fs_default",
        poke_interval=15,
        timeout=SIMULATED_DAY_SECONDS * 2,
        mode="reschedule",
    )

    locate_file = PythonOperator(
        task_id="find_latest_cost_file",
        python_callable=find_latest_cost_file,
    )

    reconcile = PythonOperator(
        task_id="reconcile_and_load",
        python_callable=reconcile_and_load,
    )

    health_check = PythonOperator(
        task_id="freshness_health_check",
        python_callable=freshness_health_check,
    )

    wait_for_cost_file >> locate_file >> reconcile
    health_check  # runs independently on its own schedule dependency-free
