"""Run in the Spark container. Includes real Parquet and historical reconciliation."""
import json
import pytest
pytest.importorskip("pyspark")
from sqlalchemy import select
from grid.domain import reading, bill
from grid.spark_common import session, canonical
from grid.store import engine, init, bills
import grid.batch as batch
from tests.test_tariffs import write


@pytest.fixture(scope="module")
def spark():
    spark = session("gridpulse-integration-tests")
    yield spark
    spark.stop()


def test_spark_validation_and_energy(spark):
    event = reading(0, 1)
    event.update(consumption_kwh=2.0, solar_generation_kwh=5.0)
    frame = spark.createDataFrame([("a", json.dumps(event)), ("b", "invalid")], ["source_id", "payload"])
    rows = canonical(frame).collect()
    assert len(rows) == 1
    assert rows[0].grid_import_kwh == 0
    assert rows[0].solar_used_kwh == 2
    assert rows[0].solar_export_kwh == 3


def test_daily_archive_recompute_and_rerun(spark, tmp_path, monkeypatch):
    monkeypatch.setattr(batch, "DATA", tmp_path)
    monkeypatch.setattr(batch, "HOUSEHOLDS", 2)
    monkeypatch.setattr(batch, "TICKS_PER_DAY", 2)
    archive = tmp_path / "raw" / "batch-000000000000"
    archive.parent.mkdir()
    events = []
    for tick in range(2):
        for h in (1, 2):
            event = reading(tick, h)
            event.update(consumption_kwh=2.0, solar_generation_kwh=5.0 if tick == 0 else 0.0)
            events.append((event["event_id"], json.dumps(event)))
    events.append(events[0])  # A replayed event must not increase consumption.
    spark.createDataFrame(events, ["source_id", "payload"]).write.parquet(str(archive))
    tariffs = tmp_path / "tariffs"
    tariffs.mkdir()
    path = tariffs / "2026-01-01.csv"
    rows = [{"simulated_date": "2026-01-01", "household_id": f"H{i:03}", "tariff_rate": 35, "subsidy_flag": str(i - 1)} for i in (1, 2)]
    write(path, rows)
    db = engine(f"sqlite:///{tmp_path / 'batch.db'}")
    init(db)
    batch.reconcile(spark, db, "2026-01-01")
    batch.reconcile(spark, db, "2026-01-01")
    with db.connect() as conn:
        result = conn.execute(select(bills).order_by(bills.c.household_id)).mappings().all()
    assert len(result) == 2
    assert result[0]["grid_import_kwh"] == 2
    assert result[0]["bill_lkr"] == bill(2, 35, False)
    assert result[1]["bill_lkr"] == bill(2, 35, True)
    rows[0]["tariff_rate"] = 40
    write(path, rows)
    batch.reconcile(spark, db, "2026-01-01")
    with db.connect() as conn:
        assert conn.execute(select(bills.c.bill_lkr).where(bills.c.household_id == "H001")).scalar() == 80
    db.dispose()

