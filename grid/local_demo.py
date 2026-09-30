"""Optional dashboard-only fixture. Does not substitute for the distributed pipeline."""
import json
from collections import defaultdict
from datetime import datetime

from grid.config import HOUSEHOLDS, TICKS_PER_DAY
from grid.domain import reading, energy, bill
from grid.logging import log
from grid.store import engine, init, ingest, save_bills, put_state


def main():
    db = engine()
    if db.dialect.name != "sqlite":
        raise RuntimeError("Offline demo requires a separate SQLite database; do not use the full pipeline database")
    init(db)
    totals = defaultdict(lambda: defaultdict(float))
    for tick in range(TICKS_PER_DAY * 2 + TICKS_PER_DAY // 2):
        events = [reading(tick, h, cloudy=tick >= TICKS_PER_DAY * 2) for h in range(1, HOUSEHOLDS + 1)]
        ingest(db, [(f"demo:{event['event_id']}", json.dumps(event)) for event in events])
        if tick < TICKS_PER_DAY * 2:
            for event in events:
                day = event["event_time"][:10]
                key = (day, event["household_id"], event["grid_zone"])
                for name in ("consumption_kwh", "solar_generation_kwh"):
                    totals[key][name] += event[name]
                for name, value in energy(event["consumption_kwh"], event["solar_generation_kwh"]).items():
                    totals[key][name] += value
                totals[key]["reading_count"] += 1
    by_day = defaultdict(list)
    for (day, hid, zone), sums in totals.items():
        subsidy = int(hid[1:]) % 5 == 0
        rate = 35 if day.endswith("01") else 37
        by_day[day].append(dict(sums, simulated_date=day, household_id=hid, grid_zone=zone,
                                tariff_rate=rate, subsidy_flag=subsidy,
                                bill_lkr=bill(sums["grid_import_kwh"], rate, subsidy)))
    for day, rows in by_day.items():
        save_bills(db, day, rows, "Offline dashboard fixture; not a Spark/Airflow execution")
    with db.begin() as conn:
        put_state(conn, "offline_fixture_created", datetime.utcnow().isoformat())
    log("offline-preview", "fixture_ready", readings=(TICKS_PER_DAY * 2 + TICKS_PER_DAY // 2) * HOUSEHOLDS)


if __name__ == "__main__":
    main()

