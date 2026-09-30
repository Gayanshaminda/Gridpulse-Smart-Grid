from fastapi import FastAPI, Query
from fastapi.responses import PlainTextResponse
from grid.store import engine, init, health
from grid.queries import daily_results, latest_readings

app = FastAPI(title="GridPulse metrics", version="1.0.0")
db = engine()
init(db)


@app.get("/health")
def get_health():
    return health(db)


@app.get("/api/live")
def live():
    return latest_readings(db)


@app.get("/api/bills")
def billing(day: str | None = Query(default=None, pattern=r"^\d{4}-\d{2}-\d{2}$")):
    return daily_results(db, day)


@app.get("/metrics", response_class=PlainTextResponse)
def metrics():
    h = health(db)
    values = {"gridpulse_accepted_readings_total": h["accepted_total"],
              "gridpulse_rejected_readings_total": h["rejected_total"],
              "gridpulse_last_reading_age_seconds": h["last_reading_age_seconds"] if h["last_reading_age_seconds"] is not None else -1,
              "gridpulse_recent_error_ratio": h["recent_error_rate"],
              "gridpulse_active_alerts": len(h["alerts"])}
    return "".join(f"# TYPE {k} gauge\n{k} {v}\n" for k, v in values.items())

