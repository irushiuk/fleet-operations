"""
Observability Watchdog
=========================
Runs independently of the API so that alerting still works even if the API
container is down (you don't want your alerting to depend on the thing it's
monitoring). Polls `pipeline_heartbeat` every CHECK_INTERVAL_SECONDS and
raises a PIPELINE_STALE alert if any component hasn't reported in N minutes.
This satisfies the "no data received in N minutes" rule from the brief.
"""
import os
import time

import psycopg2

import sys
sys.path.append("/app")
from common.logging_utils import get_logger, log_with_fields

logger = get_logger("watchdog")

PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "dbname": os.environ.get("POSTGRES_DB", "fleetdb"),
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleetpass"),
    "port": 5432,
}
CHECK_INTERVAL_SECONDS = int(os.environ.get("WATCHDOG_INTERVAL_SECONDS", "30"))
STALE_THRESHOLD_SECONDS = int(os.environ.get("STALE_THRESHOLD_SECONDS", "120"))
SIMULATED_DAY_SECONDS = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))
# The Spark job heartbeats every ~20s, but the Airflow DAG only runs once per
# simulated day, so it needs a much longer threshold or it would raise false
# PIPELINE_STALE alerts between runs.
COMPONENT_THRESHOLDS = {
    "spark_streaming_job": STALE_THRESHOLD_SECONDS,
    "airflow_daily_reconciliation": 2 * SIMULATED_DAY_SECONDS + 60,
}


def connect_with_retry():
    for attempt in range(30):
        try:
            return psycopg2.connect(**PG_CONN)
        except Exception as e:
            logger.warning(f"Postgres not ready (attempt {attempt+1}/30): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Postgres after 30 attempts")


def check_once(conn):
    cur = conn.cursor()
    cur.execute("SELECT component, last_heartbeat FROM pipeline_heartbeat")
    seen = {row[0]: row[1] for row in cur.fetchall()}

    for component, threshold in COMPONENT_THRESHOLDS.items():
        if component not in seen:
            log_with_fields(logger, "warning", "component_never_reported", component=component)
            continue
        age = (time.time() - seen[component].timestamp())
        if age > threshold:
            log_with_fields(logger, "error", "component_stale", component=component,
                            age_seconds=round(age, 1), threshold_seconds=threshold)
            cur.execute("""
                INSERT INTO alerts (alert_type, severity, subject_id, message)
                SELECT 'PIPELINE_STALE', 'CRITICAL', %s, %s
                WHERE NOT EXISTS (
                    SELECT 1 FROM alerts
                    WHERE alert_type = 'PIPELINE_STALE' AND subject_id = %s AND resolved = false
                )
            """, (component,
                  f"No heartbeat from {component} in {age:.0f}s (threshold {threshold}s)",
                  component))
        else:
            log_with_fields(logger, "info", "component_healthy", component=component,
                            age_seconds=round(age, 1))
            # component recovered -> clear any open staleness alert
            cur.execute("""UPDATE alerts SET resolved = true
                           WHERE alert_type = 'PIPELINE_STALE' AND subject_id = %s AND resolved = false""",
                        (component,))
    conn.commit()
    cur.close()


def main():
    logger.info(f"Watchdog starting: checking every {CHECK_INTERVAL_SECONDS}s, "
                f"stale threshold {STALE_THRESHOLD_SECONDS}s")
    conn = connect_with_retry()
    while True:
        try:
            check_once(conn)
        except Exception as e:
            logger.error(f"Watchdog check failed: {e}")
            try:
                conn.rollback()
            except Exception:
                conn = connect_with_retry()
        time.sleep(CHECK_INTERVAL_SECONDS)


if __name__ == "__main__":
    main()
