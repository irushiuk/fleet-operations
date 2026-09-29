"""
Simulated Daily-Batch Source — Vehicle Expense Feed
=====================================================
Represents fuel/garage partners submitting one CSV of per-vehicle running
costs per day. Drops a file into /data/batch_incoming every
SIMULATED_DAY_SECONDS (default 300s = 5 minutes, per the assignment's
"1 day = 5 minutes" compression suggestion). Airflow's file sensor (or
schedule) picks this up for the batch layer.

File naming: vehicle_costs_YYYY-MM-DD.csv
If NUM_VEHICLES doesn't change, business_date advances by 1 simulated day
each drop, starting from START_DATE (or today if unset), so the daily
reconciliation report has a clean, demoable date sequence.
"""
import csv
import os
import random
import time
from datetime import date, datetime, timedelta, timezone
import glob

import sys
sys.path.append("/app")
from common.logging_utils import get_logger, log_with_fields

logger = get_logger("batch_producer")

OUTPUT_DIR = os.environ.get("BATCH_OUTPUT_DIR", "/data/batch_incoming")
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "25"))
SIMULATED_DAY_SECONDS = float(os.environ.get("SIMULATED_DAY_SECONDS", "300"))
START_DATE_ENV = os.environ.get("START_DATE")  # YYYY-MM-DD, optional


def make_daily_file(business_date: date, period_start: datetime, period_end: datetime):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    path = os.path.join(OUTPUT_DIR, f"vehicle_costs_{business_date.isoformat()}.csv")
    rows = []
    for i in range(NUM_VEHICLES):
        vehicle_id = f"veh_{i:04d}"
        distance = round(random.uniform(20, 260), 1)
        fuel_cost = round(distance * random.uniform(0.35, 0.55), 2)
        # ~8% chance of a maintenance event that day (bigger cost spike)
        service_flag = random.random() < 0.08
        maintenance_cost = round(random.uniform(50, 400), 2) if service_flag else round(random.uniform(0, 15), 2)
        rows.append({
            "vehicle_id": vehicle_id,
            "fuel_cost": fuel_cost,
            "maintenance_cost": maintenance_cost,
            "distance_covered": distance,
            "service_flag": service_flag,
            "period_start_utc": period_start.isoformat(),
            "period_end_utc": period_end.isoformat(),
        })

    tmp_path = path + ".tmp"
    with open(tmp_path, "w", newline="") as f:
        writer = csv.DictWriter(f, fieldnames=["vehicle_id", "fuel_cost", "maintenance_cost",
                                                 "distance_covered", "service_flag",
                                                 "period_start_utc", "period_end_utc"])
        writer.writeheader()
        writer.writerows(rows)
    os.rename(tmp_path, path)  # atomic rename so Airflow never sees a half-written file
    return path, len(rows)


def main():
    logger.info(f"Starting batch producer: 1 simulated day = {SIMULATED_DAY_SECONDS}s, "
                f"writing to {OUTPUT_DIR}")
    existing = glob.glob(os.path.join(OUTPUT_DIR, "vehicle_costs_*.csv"))
    latest_date = max((date.fromisoformat(os.path.basename(path)[14:24]) for path in existing),
                      default=None)
    business_date = (date.fromisoformat(START_DATE_ENV) if START_DATE_ENV else
                     latest_date + timedelta(days=1) if latest_date else date.today())
    period_start = datetime.now(timezone.utc)

    while True:
        time.sleep(SIMULATED_DAY_SECONDS)
        period_end = datetime.now(timezone.utc)
        path, n = make_daily_file(business_date, period_start, period_end)
        log_with_fields(logger, "info", "daily_file_dropped",
                         business_date=business_date.isoformat(), rows=n, path=path)
        business_date += timedelta(days=1)
        period_start = period_end


if __name__ == "__main__":
    main()
