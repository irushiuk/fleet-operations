# EC8203 Data Engineering Mini-Project Report
## Real-Time Fleet Operations & Daily Profitability Reconciliation

> Fill in [square-bracketed] placeholders, and drop in screenshots before
> submission. Sections 2 and 4 are drafted in full — read them critically
> and adjust to match what you actually observe when you run the system;
> you must be able to defend every claim in the viva.

---

### 1. Use Case & Business Requirements

Chosen use case: **Ride-Hailing Fleet Operations** (Use Case 1).

Business question: *What is fleet utilization and earnings by zone/time-of-day
right now, and which vehicles are becoming unprofitable once yesterday's
fuel/maintenance costs are factored in?*

This decomposes into two distinct requirements with different latency needs:
- **R1 (operational, real-time):** dispatchers need current idle/active/
  on-trip counts and earnings by zone to rebalance supply — staleness of
  more than a minute or two degrades decision quality.
- **R2 (financial, daily):** finance needs a profitability view per vehicle
  once fuel/maintenance invoices are available, which happens once per
  business day regardless of pipeline design.

---

### 2. Architecture Decision: Lambda vs Kappa

**Decision: Lambda architecture.**

**Requirements analysis**

| Dimension | R1 (utilization) | R2 (profitability) |
|---|---|---|
| Latency | Seconds–minutes | Once per business day |
| Data source | Continuous stream | Once-daily file drop, externally controlled |
| Reprocessing need | Low — recent windows only | Moderate — must re-derive trip aggregates from the log every time a day's costs land, and must be redoable if a corrected cost file arrives |
| Consistency need | Approximate/eventual is fine | Needs to be exactly reconciled against the day's actual invoice figures |

**Why Lambda fits better than Kappa here:**

1. **The two requirements have genuinely different latency contracts, not
   just different current implementations.** Kappa's core premise is that
   *everything* is a stream and batch views are just a replayed/reprocessed
   stream computation. That's a good fit when all data enters through one
   continuous log and the only difference between "real-time" and
   "historical" views is how much of the log you replay. Here, R2's input
   (the cost CSV) is not itself a stream of discrete, independently
   meaningful events arriving continuously — it is a single once-a-day
   batch artifact controlled by an external partner's invoicing cycle.
   Forcing it through Kafka as a "stream" would not buy any latency benefit
   (there's nothing to consume faster than once a day) and would add
   unnecessary ingestion complexity for no observable gain.

2. **Different re-computation semantics.** R1's windowed aggregates are
   naturally computed once and discarded/rolled forward (approximate,
   monotonic). R2 requires re-deriving trip aggregates from the immutable
   event log *and* joining them against a slow-changing reference dataset
   (costs) with clear "as-of business-date" join semantics — closer to a
   classic batch ETL join than a streaming join, and easier to reason
   about, test and audit as a discrete batch job.

3. **Operational simplicity for a small team.** With Kappa, correcting a
   historical view means replaying the log through the *same* streaming
   job with different offsets — which requires careful state/checkpoint
   management and a stream processor capable of fast historical replay
   (typically requires more infrastructure investment, e.g. a
   well-tuned Flink/ksqlDB setup) to be practical. With Lambda, correcting
   a day's profitability figures is simply re-running one Airflow task —
   easier to build, test, and defend within this project's two-week scope.

**Rejected alternative — pure Kappa:** would mean pushing the daily cost
file through Kafka as well and re-running the *same* streaming job to
regenerate both views. Rejected because (a) it doesn't reduce latency for
R2 — the cost file only arrives once a day regardless of transport, and (b)
it forces R1's low-latency windowed logic and R2's exact-join reconciliation
logic into one processing model, when they have materially different
correctness requirements (approximate vs. exact) that are cleaner to keep
separate.

**Trade-offs accepted with Lambda:**
- Two codebases (streaming job + batch DAG) instead of one — more code to
  maintain, and any joined business logic that needs to appear in *both*
  layers must be kept consistent by hand (illustrated in this project: the
  idle-vehicle alert rule and the trip/earnings aggregation logic both
  reference `status`/`fare` semantics defined in the streaming job's schema).
- The batch layer re-reads from the Parquet archive rather than the
  live stream, so there is unavoidable batch latency (up to
  `SIMULATED_DAY_SECONDS`) before profitability figures are available —
  acceptable here since R2 is inherently daily.

---

### 3. Technology Stack Justification

| Layer | Choice | Justification |
|---|---|---|
| Ingestion | Apache Kafka (KRaft) | Decouples the GPS producer from the streaming consumer; partitioned by `vehicle_id` for ordered per-vehicle processing; industry-standard for this data volume/shape |
| Speed layer | Spark Structured Streaming | Native windowed aggregation (`groupBy(window(...))`) and watermarking for late data; `foreachBatch` lets one micro-batch write to both Postgres (metrics) and Parquet (archive) |
| Batch layer | Apache Airflow | Explicit DAG dependencies (sensor → locate file → reconcile → alert) match the brief's requirement to "manage batch jobs"; independent freshness-check task demonstrates observability decoupled from job success |
| Archive | Parquet on mounted volume (stands in for S3/HDFS) | Columnar format makes the daily `groupby` re-aggregation in the batch layer cheap; partitioned by date for efficient reads |
| Serving store | PostgreSQL | Relational joins/upserts needed for `vehicle_current_state` and the merged profitability report; simple to query from FastAPI |
| Serving API | FastAPI | Async, typed, auto-docs; small footprint suited to a 2-week project |
| Observability | JSON structured logs + Prometheus client + custom `alerts`/`pipeline_heartbeat` tables | Directly satisfies the brief's "structured logging" + "at least one alert rule" requirements without needing a full Grafana/Prometheus server stack for a laptop demo |

[Add 1–2 paragraphs on what you'd swap at production scale — Cassandra for
the serving store if write volume grew, MSK/Confluent Cloud for Kafka,
real S3 instead of a bind mount, Flink if sub-second latency were needed.]

---

### 4. Observability Design

| Signal | Mechanism | What it detects |
|---|---|---|
| Structured logs | `common/logging_utils.py` JSON formatter, one line per event, across every component | Debugging, audit trail of every stage |
| Metrics | `/metrics` (Prometheus format): request count + latency histograms per API endpoint | API health/performance |
| Heartbeats | `pipeline_heartbeat` table, updated by Spark job + Airflow DAG each run | "Is each pipeline stage actually still running?" |
| Alert: `VEHICLE_IDLE` | Raised in the streaming job's `foreachBatch` when a vehicle's `status_since` exceeds `IDLE_ALERT_THRESHOLD_MIN` | Operational — underutilized vehicle |
| Alert: `PIPELINE_STALE` | Checked independently by both `watchdog.py` (polling loop) and the Airflow `freshness_health_check` task | Pipeline failure — no new data in N minutes, detected even if one monitor itself is down |
| Alert: `UNPROFITABLE_VEHICLE` | Raised by the daily reconciliation task when `net_profit < 0` | Financial — surfaces exactly the business question R2 asks |

[Insert screenshot of the `alerts` table / dashboard alert panel showing at
least one of each alert type firing during your demo run.]

---

### 5. Results

[Insert screenshots: dashboard homepage, `/api/v1/fleet/utilization`
response, `/api/v1/fleet/profitability` response for at least one full
simulated day, Airflow DAG graph view showing a successful run, Kafka
topic message count / consumer lag if you capture it.]

[Include 2–3 sentences per screenshot explaining what it demonstrates.]

---

### 6. Limitations & Production Trade-offs

- Single-broker Kafka, no replication — production would need ≥3 brokers.
- No exactly-once guarantees end-to-end; `foreachBatch` writes are
  best-effort append/upsert, not transactional across Postgres+Parquet.
- No schema registry — a breaking change to the event JSON schema would
  fail silently rather than being caught at ingestion.
- [Add your own observations from actually running it — e.g. anything you
  saw fail, lag, or need tuning during your 2-week build.]

---

### 7. Individual Contributions

[Required if submitted as a group.]
