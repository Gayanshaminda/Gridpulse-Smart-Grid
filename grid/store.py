"""Small queryable serving store; PostgreSQL in Compose, SQLite for offline checks."""
import json
from datetime import datetime, timedelta
from sqlalchemy import (create_engine, MetaData, Table, Column, String, Float, Integer,
                        DateTime, Text, Boolean, select, func, delete)
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

from grid.config import DATA, DATABASE_URL
from grid.domain import energy, validate

metadata = MetaData()
readings = Table("readings", metadata,
    Column("event_id", String, primary_key=True), Column("household_id", String, nullable=False),
    Column("meter_id", String), Column("grid_zone", String, index=True),
    Column("event_time", DateTime, index=True), Column("simulated_date", String, index=True),
    Column("interval_seconds", Float), Column("consumption_kwh", Float),
    Column("solar_generation_kwh", Float), Column("solar_used_kwh", Float),
    Column("grid_import_kwh", Float), Column("solar_export_kwh", Float),
    Column("received_at", DateTime))
rejects = Table("rejected_readings", metadata,
    Column("source_id", String, primary_key=True), Column("reason", Text),
    Column("payload", Text), Column("received_at", DateTime))
state = Table("pipeline_state", metadata,
    Column("key", String, primary_key=True), Column("value", Text), Column("updated_at", DateTime))
bills = Table("daily_bills", metadata,
    Column("simulated_date", String, primary_key=True), Column("household_id", String, primary_key=True),
    Column("grid_zone", String), Column("consumption_kwh", Float), Column("solar_generation_kwh", Float),
    Column("solar_used_kwh", Float), Column("grid_import_kwh", Float), Column("solar_export_kwh", Float),
    Column("tariff_rate", Float), Column("subsidy_flag", Boolean), Column("bill_lkr", Float),
    Column("reading_count", Integer), Column("updated_at", DateTime))
runs = Table("batch_runs", metadata,
    Column("simulated_date", String, primary_key=True), Column("status", String),
    Column("detail", Text), Column("updated_at", DateTime))


def engine(url=None):
    DATA.mkdir(parents=True, exist_ok=True)
    return create_engine(url or DATABASE_URL, pool_pre_ping=True)


def init(db):
    metadata.create_all(db)


def insert(conn, table):
    return pg_insert(table) if conn.dialect.name == "postgresql" else sqlite_insert(table)


def upsert(conn, table, values, keys):
    stmt = insert(conn, table).values(**values)
    return conn.execute(stmt.on_conflict_do_update(index_elements=keys,
                        set_={k: getattr(stmt.excluded, k) for k in values if k not in keys}))


def put_state(conn, key, value):
    upsert(conn, state, {"key": key, "value": str(value), "updated_at": datetime.utcnow()}, ["key"])


def get_state(db, key, default=None):
    with db.connect() as conn:
        value = conn.execute(select(state.c.value).where(state.c.key == key)).scalar()
    return default if value is None else value


def ingest(db, envelopes, derived=None):
    accepted = rejected = duplicates = 0
    with db.begin() as conn:
        for source_id, payload in envelopes:
            try:
                obj = json.loads(payload)
                at = validate(obj)
                row = {k: obj[k] for k in ("event_id", "household_id", "meter_id", "grid_zone",
                                           "interval_seconds", "consumption_kwh", "solar_generation_kwh")}
                row.update(energy(obj["consumption_kwh"], obj["solar_generation_kwh"]))
                if derived and obj["event_id"] in derived:
                    row.update(derived[obj["event_id"]])
                row.update(event_time=at, simulated_date=at.date().isoformat(), received_at=datetime.utcnow())
                result = conn.execute(insert(conn, readings).values(**row).on_conflict_do_nothing(index_elements=["event_id"]))
                if result.rowcount:
                    accepted += 1
                else:
                    duplicates += 1
            except (ValueError, TypeError, KeyError, OverflowError) as exc:
                result = conn.execute(insert(conn, rejects).values(source_id=source_id, reason=str(exc),
                    payload=payload, received_at=datetime.utcnow()).on_conflict_do_nothing(index_elements=["source_id"]))
                rejected += result.rowcount
        put_state(conn, "stream_heartbeat", datetime.utcnow().isoformat())
        if accepted:
            put_state(conn, "last_accepted_at", datetime.utcnow().isoformat())
    return {"accepted": accepted, "rejected": rejected, "duplicates": duplicates}


def save_bills(db, day, rows, detail):
    # One transaction publishes the whole daily snapshot, including on reruns.
    with db.begin() as conn:
        conn.execute(delete(bills).where(bills.c.simulated_date == day))
        if rows:
            conn.execute(bills.insert(), [dict(r, updated_at=datetime.utcnow()) for r in rows])
        upsert(conn, runs, {"simulated_date": day, "status": "complete", "detail": detail,
                           "updated_at": datetime.utcnow()}, ["simulated_date"])


def health(db):
    now = datetime.utcnow()
    with db.connect() as conn:
        count = conn.execute(select(func.count()).select_from(readings)).scalar()
        rejected = conn.execute(select(func.count()).select_from(rejects)).scalar()
        last = conn.execute(select(func.max(readings.c.received_at))).scalar()
        recent = conn.execute(select(func.count()).select_from(readings).where(readings.c.received_at > now - timedelta(seconds=60))).scalar()
        recent_bad = conn.execute(select(func.count()).select_from(rejects).where(rejects.c.received_at > now - timedelta(seconds=60))).scalar()
        jobs = [dict(r) for r in conn.execute(select(runs)).mappings()]
    age = (now - last).total_seconds() if last else None
    alerts = []
    if age is None or age > 30:
        alerts.append("No valid meter readings received in the last 30 seconds")
    error_rate = recent_bad / max(recent + recent_bad, 1)
    if error_rate > 0.05:
        alerts.append("Rejected readings exceed 5% of arrivals in the last minute")
    for job in jobs:
        if job["status"] != "complete":
            alerts.append(f"Billing {job['simulated_date']}: {job['detail']}")
    heartbeat = get_state(db, "stream_heartbeat")
    heartbeat_age = (now - datetime.fromisoformat(heartbeat)).total_seconds() if heartbeat else None
    return {"status": "warning" if alerts else "healthy", "accepted_total": count,
            "rejected_total": rejected, "last_reading_age_seconds": age,
            "stream_heartbeat_age_seconds": heartbeat_age, "recent_error_rate": error_rate,
            "alerts": alerts, "batch_runs": jobs}
