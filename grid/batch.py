"""Airflow invokes this authoritative historical recomputation."""
import argparse
import csv
import math
from datetime import datetime, timedelta

from grid.config import DATA, HOUSEHOLDS, TICKS_PER_DAY, START_DATE
from grid.logging import log
from grid.store import engine, init, get_state, save_bills, upsert, runs


def load_tariffs(path, day):
    with path.open(encoding="utf-8", newline="") as stream:
        rows = list(csv.DictReader(stream))
    found = set()
    result = []
    expected = {f"H{h:03}" for h in range(1, HOUSEHOLDS + 1)}
    for row in rows:
        hid = row["household_id"]
        rate = float(row["tariff_rate"])
        if hid in found or hid not in expected or row["simulated_date"] != day:
            raise ValueError("Duplicate/unknown household or wrong date in tariff file")
        if not math.isfinite(rate) or rate < 0 or row["subsidy_flag"] not in ("0", "1"):
            raise ValueError("Invalid tariff or subsidy flag")
        found.add(hid)
        result.append({"household_id": hid, "tariff_rate": rate, "subsidy_flag": row["subsidy_flag"] == "1"})
    if found != expected:
        raise ValueError("Tariff file must contain exactly one row per household")
    return result


def completed_days(db):
    total = int(get_state(db, "next_tick", "0")) // TICKS_PER_DAY
    from sqlalchemy import select
    with db.connect() as conn:
        done = set(conn.execute(select(runs.c.simulated_date).where(runs.c.status == "complete")).scalars())
    # Catch up older unprocessed days first, then revisit the recent window.
    # Limit each task to three days so a long outage does not create an unbounded job.
    eligible = []
    for index in range(total):
        day = (datetime.fromisoformat(START_DATE) + timedelta(days=index)).date().isoformat()
        if day not in done or index >= total - 3:
            eligible.append(day)
    return eligible[:3]


def reconcile(spark, db, day):
    from pyspark.sql import functions as F
    from grid.spark_common import canonical
    tariff_rows = load_tariffs(DATA / "tariffs" / f"{day}.csv", day)
    paths = sorted(str(p) for p in (DATA / "raw").glob("batch-*") if (p / "_SUCCESS").exists())
    if not paths:
        raise ValueError("Waiting for archived meter readings")
    data = canonical(spark.read.parquet(*paths)).where(F.col("simulated_date") == day).dropDuplicates(["event_id"])
    sums = ["consumption_kwh", "solar_generation_kwh", "solar_used_kwh", "grid_import_kwh", "solar_export_kwh"]
    totals = data.groupBy("simulated_date", "household_id", "grid_zone").agg(
        *[F.sum(F.col(name).cast("decimal(24,6)")).alias(name) for name in sums],
        F.count("event_id").alias("reading_count"))
    totals.persist()
    try:
        coverage = totals.select("household_id", "reading_count").collect()
        expected_households = {f"H{h:03}" for h in range(1, HOUSEHOLDS + 1)}
        if (len(coverage) != HOUSEHOLDS or {r.household_id for r in coverage} != expected_households
                or any(r.reading_count != TICKS_PER_DAY for r in coverage)):
            raise ValueError(f"Waiting for complete coverage: need {HOUSEHOLDS} households x {TICKS_PER_DAY} intervals")
        tariffs = spark.createDataFrame(tariff_rows)
        joined = totals.join(tariffs, "household_id", "inner")
        joined = joined.withColumn("bill_lkr", F.round(
            F.col("grid_import_kwh") * F.col("tariff_rate").cast("decimal(12,4)") *
            F.when(F.col("subsidy_flag"), F.lit("0.8").cast("decimal(2,1)")).otherwise(F.lit("1.0").cast("decimal(2,1)")), 2))
        output = []
        for row in joined.collect():
            item = row.asDict()
            for name in sums + ["bill_lkr"]:
                item[name] = float(item[name])
            output.append(item)
        save_bills(db, day, output, f"{len(output)} household bills; rebuilt from archived readings")
        log("batch", "day_published", simulated_date=day, households=len(output))
    finally:
        totals.unpersist()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day", help="Explicit YYYY-MM-DD for older-day reconciliation")
    args = parser.parse_args()
    db = engine()
    init(db)
    days = [args.day] if args.day else completed_days(db)
    if not days:
        log("batch", "no_completed_days")
        return
    from grid.spark_common import session
    spark = session("gridpulse-batch-layer")
    failures = []
    try:
        for day in days:
            try:
                reconcile(spark, db, day)
            except Exception as exc:
                detail = str(exc)
                with db.begin() as conn:
                    upsert(conn, runs, {"simulated_date": day, "status": "blocked", "detail": detail,
                                       "updated_at": datetime.utcnow()}, ["simulated_date"])
                log("batch", "day_failed", simulated_date=day, reason=detail)
                failures.append(day)
    finally:
        spark.stop()
    if failures:
        raise RuntimeError(f"Daily reconciliation incomplete: {', '.join(failures)}")


if __name__ == "__main__":
    main()
