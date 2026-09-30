import argparse
import csv
import json
import os
import time
from datetime import datetime, timedelta

from grid.config import DATA, HOUSEHOLDS, BROKER, TOPIC, EMIT_SECONDS, START_DATE, TICKS_PER_DAY
from grid.domain import reading
from grid.logging import log
from grid.store import engine, init, get_state, put_state


def write_tariff(day, households=HOUSEHOLDS):
    folder = DATA / "tariffs"
    folder.mkdir(parents=True, exist_ok=True)
    target = folder / f"{day}.csv"
    if target.exists():
        return target
    temp = target.with_suffix(".tmp")
    day_number = (datetime.fromisoformat(day) - datetime.fromisoformat(START_DATE)).days
    with temp.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=["simulated_date", "household_id", "tariff_rate", "subsidy_flag"])
        writer.writeheader()
        for h in range(1, households + 1):
            writer.writerow({"simulated_date": day, "household_id": f"H{h:03}",
                             "tariff_rate": 35 + day_number % 3 * 2, "subsidy_flag": int(h % 5 == 0)})
    os.replace(temp, target)
    log("tariff-source", "file_published", simulated_date=day, path=target)
    return target


def meters():
    from kafka import KafkaProducer
    db = engine()
    init(db)
    producer = KafkaProducer(bootstrap_servers=BROKER, acks="all", retries=10,
                             value_serializer=lambda value: json.dumps(value).encode(),
                             key_serializer=lambda value: value.encode())
    tick = int(get_state(db, "next_tick", "0"))
    while True:
        started = time.monotonic()
        # Keep a retry deterministic even if the demo cloud switch changes.
        pending = get_state(db, "pending_tick")
        if pending and json.loads(pending)["tick"] == tick:
            cloudy = json.loads(pending)["cloudy"]
        else:
            cloudy = get_state(db, "cloudy", "false") == "true"
            with db.begin() as conn:
                put_state(conn, "pending_tick", json.dumps({"tick": tick, "cloudy": cloudy}))
        futures = [producer.send(TOPIC, key=f"H{h:03}", value=reading(tick, h, cloudy))
                   for h in range(1, HOUSEHOLDS + 1)]
        for future in futures:
            future.get(timeout=30)
        producer.flush()
        tick += 1
        with db.begin() as conn:
            put_state(conn, "next_tick", tick)
            put_state(conn, "producer_heartbeat", datetime.utcnow().isoformat())
        log("meter-source", "tick_sent", tick=tick, events=HOUSEHOLDS, cloudy=cloudy)
        time.sleep(max(0, EMIT_SECONDS - (time.monotonic() - started)))


def tariffs():
    db = engine()
    init(db)
    while True:
        completed = int(get_state(db, "next_tick", "0")) // TICKS_PER_DAY
        for day_index in range(completed):
            day = (datetime.fromisoformat(START_DATE) + timedelta(days=day_index)).date().isoformat()
            write_tariff(day)
        with db.begin() as conn:
            put_state(conn, "tariff_heartbeat", datetime.utcnow().isoformat())
        time.sleep(3)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("source", choices=["meters", "tariffs"])
    args = parser.parse_args()
    (meters if args.source == "meters" else tariffs)()
