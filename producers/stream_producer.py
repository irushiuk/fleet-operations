"""
Simulated Streaming Source — Ride-Hailing GPS/Telemetry Feed
==============================================================
Emits one event every EMIT_INTERVAL_SECONDS per active "vehicle" to the
Kafka topic `gps_events`. Each event represents a telemetry ping:

    trip_id, driver_id, vehicle_id, lat, lon, speed, status, fare, timestamp

This deliberately models real fleet behaviour with a simple per-vehicle
state machine (idle -> enroute -> on_trip -> idle ...) so downstream
aggregations (utilization, idle ratio, earnings) are meaningful rather than
random noise.
"""
import json
import os
import random
import time
import uuid
from datetime import datetime, timezone

from kafka import KafkaProducer

import sys
sys.path.append("/app")
from common.logging_utils import get_logger, log_with_fields

logger = get_logger("stream_producer")

KAFKA_BOOTSTRAP = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = os.environ.get("GPS_TOPIC", "gps_events")
NUM_VEHICLES = int(os.environ.get("NUM_VEHICLES", "25"))
EMIT_INTERVAL_SECONDS = float(os.environ.get("EMIT_INTERVAL_SECONDS", "3"))
# Simulated "parked / driver on break" vehicles: they never leave the idle state,
# which guarantees the idle-vehicle alert rule can be demonstrated.
PARKED_VEHICLES = int(os.environ.get("PARKED_VEHICLES", "3"))

ZONES = {
    "downtown":  {"lat": (6.925, 6.935), "lon": (79.845, 79.855)},
    "airport":   {"lat": (7.170, 7.185), "lon": (79.880, 79.895)},
    "suburbs_n": {"lat": (6.960, 6.980), "lon": (79.860, 79.880)},
    "suburbs_s": {"lat": (6.870, 6.890), "lon": (79.855, 79.870)},
}
ZONE_NAMES = list(ZONES.keys())

STATUSES = ["idle", "enroute", "on_trip"]
# transition probabilities: idle -> enroute -> on_trip -> idle (mostly)
TRANSITIONS = {
    "idle":    {"idle": 0.55, "enroute": 0.45},
    "enroute": {"enroute": 0.35, "on_trip": 0.55, "idle": 0.10},
    "on_trip": {"on_trip": 0.70, "idle": 0.30},
}


class Vehicle:
    def __init__(self, vehicle_id: str, parked: bool = False):
        self.parked = parked
        self.vehicle_id = vehicle_id
        self.driver_id = f"drv_{vehicle_id[-4:]}"
        self.zone = random.choice(ZONE_NAMES)
        bounds = ZONES[self.zone]
        self.lat = random.uniform(*bounds["lat"])
        self.lon = random.uniform(*bounds["lon"])
        self.status = "idle"
        self.trip_id = None

    def _event(self, speed, fare):
        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "zone": self.zone,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": round(speed, 1),
            "status": self.status,
            "fare": fare,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }

    def step(self):
        if self.parked:
            self.status, self.trip_id = "idle", None
            return self._event(0.0, 0.0)
        # occasionally hop zones (simulates travel between areas)
        if random.random() < 0.03:
            self.zone = random.choice(ZONE_NAMES)

        bounds = ZONES[self.zone]
        # small random walk within the zone's bounding box
        self.lat = min(max(self.lat + random.uniform(-0.002, 0.002), bounds["lat"][0]), bounds["lat"][1])
        self.lon = min(max(self.lon + random.uniform(-0.002, 0.002), bounds["lon"][0]), bounds["lon"][1])

        prev_status = self.status
        probs = TRANSITIONS[self.status]
        self.status = random.choices(list(probs.keys()), weights=list(probs.values()))[0]

        if self.status == "enroute" and prev_status != "enroute":
            self.trip_id = f"trip_{uuid.uuid4().hex[:10]}"
        if self.status == "idle":
            self.trip_id = None

        speed = 0.0
        fare = 0.0
        if self.status == "enroute":
            speed = random.uniform(15, 45)
        elif self.status == "on_trip":
            speed = random.uniform(10, 60)
            fare = round(random.uniform(0.5, 3.5), 2)  # incremental fare accrued this tick
        # idle vehicles: speed 0, occasionally a tiny creep
        elif self.status == "idle" and random.random() < 0.1:
            speed = random.uniform(0, 5)

        return {
            "trip_id": self.trip_id,
            "driver_id": self.driver_id,
            "vehicle_id": self.vehicle_id,
            "zone": self.zone,
            "lat": round(self.lat, 6),
            "lon": round(self.lon, 6),
            "speed": round(speed, 1),
            "status": self.status,
            "fare": fare,
            "timestamp": datetime.now(timezone.utc).isoformat(),
        }


def build_producer() -> KafkaProducer:
    for attempt in range(30):
        try:
            return KafkaProducer(
                bootstrap_servers=KAFKA_BOOTSTRAP,
                value_serializer=lambda v: json.dumps(v).encode("utf-8"),
                key_serializer=lambda k: k.encode("utf-8") if k else None,
                linger_ms=200,
                retries=5,
            )
        except Exception as e:
            logger.warning(f"Kafka not ready yet (attempt {attempt+1}/30): {e}")
            time.sleep(3)
    raise RuntimeError("Could not connect to Kafka after 30 attempts")


def main():
    logger.info(f"Starting stream producer: {NUM_VEHICLES} vehicles, "
                f"emitting every {EMIT_INTERVAL_SECONDS}s to topic '{TOPIC}'")
    producer = build_producer()
    vehicles = [Vehicle(f"veh_{i:04d}", parked=(i >= NUM_VEHICLES - PARKED_VEHICLES))
                for i in range(NUM_VEHICLES)]

    total_sent = 0
    while True:
        batch_start = time.time()
        for v in vehicles:
            event = v.step()
            producer.send(TOPIC, key=event["vehicle_id"], value=event)
            total_sent += 1
        producer.flush()
        log_with_fields(logger, "info", "batch_emitted",
                         events_this_tick=len(vehicles), total_sent=total_sent)
        elapsed = time.time() - batch_start
        time.sleep(max(0.0, EMIT_INTERVAL_SECONDS - elapsed))


if __name__ == "__main__":
    main()
