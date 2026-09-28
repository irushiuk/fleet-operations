"""
Serving Layer API
===================
Exposes the Lambda architecture's merged view to consumers:
  * /api/v1/fleet/utilization  -> speed-layer real-time metrics
  * /api/v1/fleet/vehicles     -> live per-vehicle state (map view)
  * /api/v1/fleet/profitability-> batch-layer daily reconciliation report
  * /api/v1/alerts             -> observability: active alerts
  * /health                    -> liveness + pipeline-freshness check
  * /metrics                   -> Prometheus scrape endpoint
  * /                          -> lightweight HTML/JS dashboard
"""
import functools
import os
import time
from datetime import datetime, timedelta

import psycopg2
import psycopg2.extras
from fastapi import FastAPI, HTTPException
from fastapi.responses import HTMLResponse
from fastapi.staticfiles import StaticFiles
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from starlette.responses import Response

import sys
sys.path.append("/app")
from common.logging_utils import get_logger, log_with_fields

logger = get_logger("serving_api")

PG_CONN = {
    "host": os.environ.get("POSTGRES_HOST", "postgres"),
    "dbname": os.environ.get("POSTGRES_DB", "fleetdb"),
    "user": os.environ.get("POSTGRES_USER", "fleet"),
    "password": os.environ.get("POSTGRES_PASSWORD", "fleetpass"),
    "port": int(os.environ.get("POSTGRES_PORT", "5432")),
}
STALE_THRESHOLD_SECONDS = int(os.environ.get("STALE_THRESHOLD_SECONDS", "120"))
SIMULATED_DAY_SECONDS = int(os.environ.get("SIMULATED_DAY_SECONDS", "300"))


def threshold_for(component: str) -> int:
    # the Airflow DAG only runs once per simulated day, so allow 2 days + margin
    if component.startswith("airflow"):
        return 2 * SIMULATED_DAY_SECONDS + 60
    return STALE_THRESHOLD_SECONDS

app = FastAPI(title="Fleet Operations Serving API", version="1.0.0")

REQUEST_COUNT = Counter("api_requests_total", "Total API requests", ["endpoint"])
REQUEST_LATENCY = Histogram("api_request_latency_seconds", "Request latency", ["endpoint"])


def get_conn():
    return psycopg2.connect(**PG_CONN, cursor_factory=psycopg2.extras.RealDictCursor)


def timed(endpoint_name):
    def deco(fn):
        @functools.wraps(fn)
        def wrapper(*args, **kwargs):
            REQUEST_COUNT.labels(endpoint=endpoint_name).inc()
            start = time.time()
            try:
                return fn(*args, **kwargs)
            finally:
                REQUEST_LATENCY.labels(endpoint=endpoint_name).observe(time.time() - start)
        return wrapper
    return deco


@app.get("/health")
def health():
    """Liveness + pipeline freshness. Returns 200 with status flags rather
    than failing outright, so the dashboard can render a warning banner."""
    try:
        conn = get_conn()
        cur = conn.cursor()
        cur.execute("SELECT component, last_heartbeat FROM pipeline_heartbeat")
        heartbeats = cur.fetchall()
        cur.close()
        conn.close()
    except Exception as e:
        logger.error(f"DB connection failed in health check: {e}")
        raise HTTPException(status_code=503, detail="database unreachable")

    now = datetime.utcnow()
    components = {}
    overall_ok = True
    for hb in heartbeats:
        age = (now - hb["last_heartbeat"].replace(tzinfo=None)).total_seconds()
        ok = age < threshold_for(hb["component"])
        overall_ok = overall_ok and ok
        components[hb["component"]] = {"last_heartbeat": hb["last_heartbeat"].isoformat(),
                                        "age_seconds": round(age, 1), "fresh": ok}
    return {"status": "ok" if overall_ok else "degraded", "components": components}


@app.get("/metrics")
def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)


@app.get("/api/v1/fleet/utilization")
@timed("utilization")
def fleet_utilization(minutes: int = 15):
    """Real-time fleet utilization per zone.

    * Vehicle counts (active / idle / enroute / on_trip, idle_ratio) are an exact
      CURRENT snapshot from vehicle_current_state: one row per vehicle, so the
      counts always add up to the fleet size.
    * Earnings and trips are summed over the last `minutes` from the speed
      layer's windowed metrics (micro-batch rows cover disjoint event slices,
      so summing them is correct; trips_started is approximate because a trip
      spanning two micro-batches can be counted in both).
    """
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT zone,
               COUNT(*) FILTER (WHERE status <> 'idle')     AS active_vehicles,
               COUNT(*) FILTER (WHERE status = 'idle')      AS idle_vehicles,
               COUNT(*) FILTER (WHERE status = 'enroute')   AS enroute_vehicles,
               COUNT(*) FILTER (WHERE status = 'on_trip')   AS on_trip_vehicles
        FROM vehicle_current_state
        WHERE updated_at > now() - interval '2 minutes'
        GROUP BY zone
    """)
    snapshot = {r["zone"]: dict(r) for r in cur.fetchall()}
    cur.execute("""
        SELECT zone,
               COALESCE(SUM(total_earnings), 0) AS earnings,
               COALESCE(SUM(trips_started), 0)  AS trips_started
        FROM vehicle_realtime_metrics
        WHERE window_start > now() - make_interval(mins => %s)
        GROUP BY zone
    """, (minutes,))
    activity = {r["zone"]: dict(r) for r in cur.fetchall()}
    cur.close()
    conn.close()

    zones = []
    for zone in sorted(set(snapshot) | set(activity)):
        snap = snapshot.get(zone, {})
        act = activity.get(zone, {})
        active = snap.get("active_vehicles", 0)
        idle = snap.get("idle_vehicles", 0)
        trips = int(act.get("trips_started", 0))
        zones.append({
            "zone": zone,
            "active_vehicles": active,
            "idle_vehicles": idle,
            "enroute_vehicles": snap.get("enroute_vehicles", 0),
            "on_trip_vehicles": snap.get("on_trip_vehicles", 0),
            "idle_ratio": round(idle / (active + idle), 3) if (active + idle) else 0.0,
            "earnings": round(float(act.get("earnings", 0.0)), 2),
            "trips_started": trips,
            "trips_per_hour": round(trips * 60 / minutes, 1),
        })
    fleet_active = sum(z["active_vehicles"] for z in zones)
    fleet_idle = sum(z["idle_vehicles"] for z in zones)
    fleet = {
        "vehicles": fleet_active + fleet_idle,
        "active_vehicles": fleet_active,
        "idle_vehicles": fleet_idle,
        "idle_ratio": round(fleet_idle / (fleet_active + fleet_idle), 3) if (fleet_active + fleet_idle) else 0.0,
        "earnings": round(sum(z["earnings"] for z in zones), 2),
    }
    log_with_fields(logger, "info", "utilization_queried", zones=len(zones), minutes=minutes)
    return {"window_minutes": minutes, "fleet": fleet, "zones": zones}


@app.get("/api/v1/fleet/utilization/history")
@timed("utilization_history")
def fleet_utilization_history(minutes: int = 15):
    """Raw speed-layer window rows (time series), newest first."""
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT zone, window_start, window_end, active_vehicles, idle_vehicles,
               enroute_vehicles, on_trip_vehicles, idle_ratio, trips_started,
               total_earnings, avg_speed, computed_at
        FROM vehicle_realtime_metrics
        WHERE window_start > now() - make_interval(mins => %s)
        ORDER BY window_start DESC, zone, computed_at DESC
    """, (minutes,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"window_minutes": minutes, "metrics": rows}


@app.get("/api/v1/fleet/vehicles")
@timed("vehicles")
def fleet_vehicles():
    conn = get_conn()
    cur = conn.cursor()
    cur.execute("""
        SELECT vehicle_id, driver_id, status, zone, lat, lon, speed,
               last_event_time, status_since
        FROM vehicle_current_state
        ORDER BY vehicle_id
    """)
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"count": len(rows), "vehicles": rows}


@app.get("/api/v1/fleet/profitability")
@timed("profitability")
def fleet_profitability(business_date: str | None = None):
    conn = get_conn()
    cur = conn.cursor()
    if business_date:
        cur.execute("""SELECT * FROM vehicle_profitability_report
                        WHERE business_date = %s ORDER BY net_profit ASC""", (business_date,))
    else:
        cur.execute("""SELECT * FROM vehicle_profitability_report
                        WHERE business_date = (SELECT MAX(business_date) FROM vehicle_profitability_report)
                        ORDER BY net_profit ASC""")
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"business_date": business_date or "latest", "report": rows}


@app.get("/api/v1/alerts")
@timed("alerts")
def alerts(active_only: bool = True, limit: int = 50):
    conn = get_conn()
    cur = conn.cursor()
    if active_only:
        cur.execute("""SELECT * FROM alerts WHERE resolved = false
                        ORDER BY raised_at DESC LIMIT %s""", (limit,))
    else:
        cur.execute("""SELECT * FROM alerts ORDER BY raised_at DESC LIMIT %s""", (limit,))
    rows = cur.fetchall()
    cur.close()
    conn.close()
    return {"count": len(rows), "alerts": rows}


@app.get("/", response_class=HTMLResponse)
def dashboard():
    with open(os.path.join(os.path.dirname(__file__), "static", "dashboard.html")) as f:
        return f.read()