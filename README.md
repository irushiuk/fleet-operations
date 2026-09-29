## Quick Start

### Prerequisites
- Docker Desktop, running, with at least 4 CPUs and 8 GB memory (Settings → Resources)
- Free ports: 8000 (API/dashboard), 8080 (Airflow), 9092 (Kafka), 5433 (Postgres)

### 1. Start the pipeline
From the project root (the folder containing `docker-compose.yml`):

```bash
docker compose up --build
```

The first run takes several minutes while images build and Spark downloads its
Kafka/Postgres connectors. Wait until the logs show `Streaming query started`.
(`kafka-init` exits with code 0 after creating the topic. This is expected.)

### 2. Open the dashboard

| What | URL |
|---|---|
| **Dashboard** | http://localhost:8000/ |
| Live utilization API | http://localhost:8000/api/v1/fleet/utilization |
| Vehicle states API | http://localhost:8000/api/v1/fleet/vehicles |
| Daily profitability API | http://localhost:8000/api/v1/fleet/profitability |
| Alerts API | http://localhost:8000/api/v1/alerts |
| Health check | http://localhost:8000/health |
| Prometheus metrics | http://localhost:8000/metrics |
| Airflow UI | http://localhost:8080 |

Expected timeline: live zone metrics appear after ~2 minutes; idle-vehicle
alerts after ~3 minutes; the first cost file is created after ~5 minutes
(1 simulated day = 5 real minutes). Airflow then processes that file on its
next scheduled run, so the first profitability report may take longer.
The dashboard shows earnings over one simulated day and trips per simulated hour.

### 3. Inspect the batch layer (Airflow)
1. Get the Airflow password: `docker compose logs airflow | grep -i password`
2. Log in at http://localhost:8080 with username `admin`.
3. Check that `daily_fleet_reconciliation` is active. The `airflow-unpause`
   setup service enables it automatically, including for an existing metadata
   database. Confirm `reconcile_and_load` succeeds before using the report.

Each new cost CSV includes the UTC start and end of its simulated day. Airflow
uses those fixed bounds when rerunning a file; old CSV files without these
columns use the file modification time as a compatibility fallback. The
business date is simulated and can advance faster than the real calendar.

### 4. Useful commands

```bash
docker compose ps                              # status of all containers
docker compose logs -f <service>               # follow logs, e.g. spark-streaming, watchdog, api
docker compose up -d --build <service>         # rebuild and restart one service after code changes
docker compose down                            # stop everything (keeps data)
docker compose down -v                         # stop and delete all data (fresh start)
```

When upgrading from an older run, restart the producer and Spark services so
the new `trip_started` field is emitted and parsed. Previously stored metrics
and alerts remain in PostgreSQL until you intentionally reset the data. Use
`docker compose down -v` only if you want to discard the old database and
Kafka data; the bind-mounted CSV, Parquet and checkpoint directories are
separate and must be cleared separately for a completely fresh demonstration.

### 5. Demo the health alert
```bash
docker compose stop spark-streaming    # after ~2 min a PIPELINE_STALE alert appears
docker compose start spark-streaming   # the alert clears once Spark reports again
```

### Query the database directly (optional)
```bash
docker exec -it fleet_postgres psql -U fleet -d fleetdb
```
From your host machine, Postgres is exposed on port **5433** (user `fleet`, database `fleetdb`).
