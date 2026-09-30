import json
from datetime import datetime, timedelta
import pytest
from sqlalchemy import select, func
from grid.domain import reading
from grid.store import engine, init, ingest, readings, rejects, bills, save_bills, health


@pytest.fixture
def db(tmp_path):
    db = engine(f"sqlite:///{tmp_path / 'test.db'}")
    init(db)
    yield db
    db.dispose()


def test_retry_and_duplicate_event_are_idempotent(db):
    value = json.dumps(reading(0, 1))
    assert ingest(db, [("topic:0:0", value)])["accepted"] == 1
    assert ingest(db, [("topic:0:0", value), ("topic:0:1", value)])["duplicates"] == 2
    with db.connect() as conn:
        assert conn.execute(select(func.count()).select_from(readings)).scalar() == 1


def test_invalid_records_are_quarantined_once(db):
    payloads = [("a", "bad json"), ("b", "null"), ("c", "{}")]
    assert ingest(db, payloads)["rejected"] == 3
    assert ingest(db, payloads)["rejected"] == 0
    with db.connect() as conn:
        assert conn.execute(select(func.count()).select_from(rejects)).scalar() == 3


def test_daily_reconciliation_replaces_old_snapshot(db):
    row = {"simulated_date": "2026-01-01", "household_id": "H001", "bill_lkr": 35}
    save_bills(db, "2026-01-01", [row], "first")
    save_bills(db, "2026-01-01", [dict(row, bill_lkr=70)], "rerun")
    with db.connect() as conn:
        results = conn.execute(select(bills)).mappings().all()
    assert len(results) == 1
    assert results[0]["bill_lkr"] == 70


def test_no_data_rule_uses_real_arrival_time(db):
    assert health(db)["alerts"]
    ingest(db, [("a", json.dumps(reading(0, 1)))])
    assert not health(db)["alerts"]
    with db.begin() as conn:
        conn.execute(readings.update().values(received_at=datetime.utcnow() - timedelta(seconds=31)))
    assert "30 seconds" in health(db)["alerts"][0]


def test_batch_discovery_catches_up_old_unprocessed_days(db):
    from grid.batch import completed_days
    from grid.store import put_state
    from grid.config import TICKS_PER_DAY
    with db.begin() as conn:
        put_state(conn, "next_tick", TICKS_PER_DAY * 5)
    assert completed_days(db) == ["2026-01-01", "2026-01-02", "2026-01-03"]
    for day in completed_days(db):
        save_bills(db, day, [], "test completion")
    assert completed_days(db) == ["2026-01-03", "2026-01-04", "2026-01-05"]
