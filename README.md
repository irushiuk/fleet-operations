# Fleet Operations Data Platform — EC8203 Mini Project

A Lambda-architecture data pipeline for real-time ride-hailing fleet
utilization monitoring, reconciled daily against vehicle running costs to
surface per-vehicle profitability.

## 1. Architecture

```
                         ┌─────────────────────────────────────────┐
                         │              SPEED LAYER                 │
 stream_producer.py ───▶ │  Kafka (gps_events) ─▶ Spark Structured  │──┐
 (GPS/telemetry, 3s)     │  Streaming: 1-min windowed aggregation,   │  │
                         │  vehicle state, idle-vehicle alert rule   │  │
                         └─────────────────────────────────────────┘  │
                                        │ archives raw events           │
                                        ▼                               ▼
                         ┌─────────────────────┐        ┌─────────────────────────┐
                         │  Parquet archive     │        │ vehicle_realtime_metrics │
                         │  (immutable log,     │        │ vehicle_current_state    │  (Postgres)
                         │  stands in for S3)    │        └─────────────────────────┘
                         └─────────────────────┘
                                        │ replayed daily
                                        ▼
                         ┌─────────────────────────────────────────┐
 batch_producer.py ────▶ │              BATCH LAYER                  │
 (cost CSV, 1/sim.day)   │  Airflow DAG: join daily cost feed with   │
                         │  trip aggregates re-derived from Parquet  │
                         └─────────────────────────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │ vehicle_profitability_report  │  (Postgres — Lambda MERGE)
                         └─────────────────────────────┘
                                        │
                                        ▼
                         ┌─────────────────────────────┐
                         │  SERVING LAYER: FastAPI +      │
                         │  HTML/JS dashboard, /metrics   │
                         └─────────────────────────────┘

   watchdog.py (independent) ──▶ polls pipeline_heartbeat ──▶ alerts table
```

**Why Lambda, not Kappa:** utilization needs sub-minute freshness (speed
layer); cost reconciliation is fundamentally once-a-day because that's how
often fuel/garage partners actually invoice — there is nothing to replay
faster. The two layers have different correctness/latency/replay
requirements, which is the textbook condition for choosing Lambda over
Kappa. Full justification, including the rejected Kappa alternative, is in
`docs/report.md` §2.

## 2. Simulated Clock

**1 simulated day = 5 minutes** (`SIMULATED_DAY_SECONDS=300` in
`docker-compose.yml`). The batch producer drops one `vehicle_costs_*.csv`
every 5 minutes with an incrementing business date; the Airflow DAG runs on
the same interval. Change `SIMULATED_DAY_SECONDS` to compress/expand the
demo.

## 3. Quickstart

Requirements: Docker Desktop (4 CPU / 8GB RAM recommended — Spark + Airflow
are the heavy containers).

```bash
git clone <your-repo-url> fleet-pipeline
cd fleet-pipeline
docker compose up --build
```

First boot takes a few minutes (Spark pulls Kafka/Postgres JDBC packages on
first `spark-submit`; Airflow initializes its metadata DB). Watch for:

```
fleet_spark_streaming  | Streaming query started
```

Then open:

| What | URL |
|---|---|
| **Dashboard** | http://localhost:8000/ |
| API root | http://localhost:8000/api/v1/fleet/utilization |
| Health check | http://localhost:8000/health |
| Prometheus metrics | http://localhost:8000/metrics |
| Airflow UI | http://localhost:8080 (user `admin`; password is printed in `docker compose logs airflow`, or in `/opt/airflow/simple_auth_manager_passwords.json.generated` / `standalone_admin_password.txt` inside the container) |

Give it ~2 minutes for the first 1-minute streaming window to populate and
~5 minutes for the first daily reconciliation to run.

To stop: `docker compose down` (add `-v` to also wipe volumes/data).

## 4. Repo Layout

```
producers/          simulated streaming + batch data sources
spark_jobs/          speed-layer Structured Streaming job
airflow/dags/         batch-layer daily reconciliation DAG
api/                  FastAPI serving layer + dashboard
watchdog/             standalone freshness/alerting loop
sql/init.sql           Postgres schema (auto-applied on first boot)
common/                shared structured JSON logging
data/                  bind-mounted volumes: batch CSVs, Parquet archive, checkpoints
docs/                  architecture diagram source, report
```

## 5. Observability

- **Structured logging**: every component logs single-line JSON via
  `common/logging_utils.py` — `docker compose logs -f | grep component`.
- **Metrics**: `/metrics` (Prometheus format) on the API; request counts +
  latency histograms per endpoint.
- **Alert rules** (written to the `alerts` table, visible on the dashboard):
  - `VEHICLE_IDLE` — a vehicle idle beyond `IDLE_ALERT_THRESHOLD_MIN`.
  - `PIPELINE_STALE` — no heartbeat from a component within
    `STALE_THRESHOLD_SECONDS` (checked independently by both the watchdog
    and the Airflow freshness task, so alerting survives either one being down).
  - `UNPROFITABLE_VEHICLE` — raised by the daily reconciliation job.
- **Heartbeats**: `pipeline_heartbeat` table, updated by the Spark job and
  the Airflow DAG on every successful run.

## 6. Known Simplifications (state these in your report)

- Parquet on a bind-mounted volume stands in for an S3/HDFS data lake —
  reasonable for a laptop demo; in production this would be real object
  storage with lifecycle policies.
- Airflow runs in `standalone` mode (webserver+scheduler in one process)
  with `LocalExecutor` to keep the container count laptop-friendly.
- Kafka runs in single-broker KRaft mode (no Zookeeper, no replication).
- The idle-alert and staleness thresholds are set low for demo purposes;
  justify realistic production values in the report.

