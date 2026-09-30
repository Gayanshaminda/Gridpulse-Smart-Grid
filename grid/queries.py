from sqlalchemy import select, func
from grid.store import readings, bills


def latest_readings(db):
    with db.connect() as conn:
        latest = conn.execute(select(func.max(readings.c.event_time))).scalar()
        if latest is None:
            return []
        return [dict(row) for row in conn.execute(select(readings).where(readings.c.event_time == latest)).mappings()]


def daily_results(db, day=None):
    query = select(bills).order_by(bills.c.simulated_date.desc(), bills.c.household_id)
    if day:
        query = query.where(bills.c.simulated_date == day)
    with db.connect() as conn:
        return [dict(row) for row in conn.execute(query).mappings()]

