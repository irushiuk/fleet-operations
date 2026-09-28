-- ==========================================================================
-- Fleet Operations Data Platform — Serving Layer Schema
-- ==========================================================================
-- Design note: this schema deliberately keeps SPEED-LAYER and BATCH-LAYER
-- output in separate tables (vehicle_realtime_metrics / vehicle_daily_costs)
-- and materializes the Lambda "merge" step explicitly into
-- vehicle_profitability_report. This makes the Lambda architecture visible
-- in the data model itself, not just in the code.

CREATE TABLE IF NOT EXISTS vehicle_realtime_metrics (
    id                  BIGSERIAL PRIMARY KEY,
    window_start        TIMESTAMP NOT NULL,
    window_end          TIMESTAMP NOT NULL,
    zone                TEXT NOT NULL,
    active_vehicles     INTEGER NOT NULL,
    idle_vehicles       INTEGER NOT NULL,
    enroute_vehicles    INTEGER NOT NULL,
    on_trip_vehicles    INTEGER NOT NULL,
    idle_ratio          DOUBLE PRECISION NOT NULL,
    trips_started       INTEGER NOT NULL,
    total_earnings      DOUBLE PRECISION NOT NULL,
    avg_speed           DOUBLE PRECISION,
    computed_at         TIMESTAMP NOT NULL DEFAULT now()
);
CREATE INDEX IF NOT EXISTS idx_rt_window ON vehicle_realtime_metrics (window_start DESC);
CREATE INDEX IF NOT EXISTS idx_rt_zone ON vehicle_realtime_metrics (zone);

-- Per-vehicle current state, upserted on every micro-batch (drives the
-- idle-vehicle alert and the "active fleet" view)
CREATE TABLE IF NOT EXISTS vehicle_current_state (
    vehicle_id          TEXT PRIMARY KEY,
    driver_id           TEXT,
    status              TEXT,
    zone                TEXT,
    lat                 DOUBLE PRECISION,
    lon                 DOUBLE PRECISION,
    speed               DOUBLE PRECISION,
    last_event_time     TIMESTAMP,
    status_since         TIMESTAMP,
    updated_at          TIMESTAMP NOT NULL DEFAULT now()
);

-- BATCH LAYER: daily vehicle expense feed, loaded verbatim by Airflow
CREATE TABLE IF NOT EXISTS vehicle_daily_costs (
    id                  BIGSERIAL PRIMARY KEY,
    business_date       DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    fuel_cost           DOUBLE PRECISION NOT NULL,
    maintenance_cost    DOUBLE PRECISION NOT NULL,
    distance_covered    DOUBLE PRECISION NOT NULL,
    service_flag        BOOLEAN NOT NULL DEFAULT false,
    loaded_at           TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (business_date, vehicle_id)
);

-- MERGE / SERVING VIEW OF LAMBDA ARCHITECTURE:
-- batch-layer costs joined with the trip archive (Parquet, aggregated by
-- Airflow) to produce the daily profitability reconciliation report.
CREATE TABLE IF NOT EXISTS vehicle_profitability_report (
    id                  BIGSERIAL PRIMARY KEY,
    business_date       DATE NOT NULL,
    vehicle_id          TEXT NOT NULL,
    total_trips         INTEGER NOT NULL,
    total_earnings      DOUBLE PRECISION NOT NULL,
    fuel_cost           DOUBLE PRECISION NOT NULL,
    maintenance_cost    DOUBLE PRECISION NOT NULL,
    net_profit          DOUBLE PRECISION NOT NULL,
    profit_margin_pct   DOUBLE PRECISION,
    is_unprofitable     BOOLEAN NOT NULL DEFAULT false,
    generated_at        TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (business_date, vehicle_id)
);

-- OBSERVABILITY: alert log written by the streaming job and the watchdog
CREATE TABLE IF NOT EXISTS alerts (
    id                  BIGSERIAL PRIMARY KEY,
    alert_type          TEXT NOT NULL,        -- 'VEHICLE_IDLE' | 'PIPELINE_STALE' | 'UNPROFITABLE_VEHICLE'
    severity            TEXT NOT NULL,        -- 'WARNING' | 'CRITICAL'
    subject_id          TEXT,                 -- vehicle_id or pipeline component name
    message             TEXT NOT NULL,
    raised_at           TIMESTAMP NOT NULL DEFAULT now(),
    resolved            BOOLEAN NOT NULL DEFAULT false
);
CREATE INDEX IF NOT EXISTS idx_alerts_raised ON alerts (raised_at DESC);

-- Pipeline heartbeat table: every producer / spark job updates its row so
-- the watchdog can detect "no data received in N minutes"
CREATE TABLE IF NOT EXISTS pipeline_heartbeat (
    component           TEXT PRIMARY KEY,
    last_heartbeat       TIMESTAMP NOT NULL DEFAULT now(),
    events_processed_total BIGINT NOT NULL DEFAULT 0
);
