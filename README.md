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
alerts after ~3 minutes; the first daily reconciliation after ~5 minutes
(1 simulated day = 5 real minutes).

### 3. Enable the batch layer (Airflow)
1. Get the Airflow password: `docker compose logs airflow | grep -i password`
2. Log in at http://localhost:8080 with username `admin`.
3. Switch on the `daily_fleet_reconciliation` DAG (DAGs start paused).
   The profitability report on the dashboard fills after its first run.

### 4. Useful commands

```bash
docker compose ps                              # status of all containers
docker compose logs -f <service>               # follow logs, e.g. spark-streaming, watchdog, api
docker compose up -d --build <service>         # rebuild and restart one service after code changes
docker compose down                            # stop everything (keeps data)
docker compose down -v                         # stop and delete all data (fresh start)
```

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