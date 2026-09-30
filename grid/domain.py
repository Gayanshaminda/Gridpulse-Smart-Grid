"""Pure, independently testable business rules. Energy is per interval."""
import math
from datetime import datetime, timedelta, timezone
from decimal import Decimal, ROUND_HALF_UP

from grid.config import INTERVAL_SECONDS, START_DATE, ZONES


def reading(tick, household, cloudy=False):
    start = datetime.fromisoformat(START_DATE).replace(tzinfo=timezone.utc)
    at = start + timedelta(seconds=tick * INTERVAL_SECONDS)
    hour = at.hour + at.minute / 60
    # Deterministic patterns make retries reproduce identical readings.
    evening = math.exp(-((hour - 19) / 2.5) ** 2)
    morning = math.exp(-((hour - 7) / 2) ** 2)
    demand_kw = (0.25 + household % 7 * 0.055) + 0.9 * evening + 0.45 * morning
    demand_kw *= 1 + 0.08 * math.sin(tick + household)
    solar_kw = max(0, math.sin(math.pi * (hour - 6) / 12)) * (1.4 + household % 4 * 0.3)
    if household % 3 == 0:
        solar_kw = 0
    if cloudy:
        solar_kw *= 0.03
    return {"event_id": f"{START_DATE}:{tick}:{household}",
            "meter_id": f"M{household:03}", "household_id": f"H{household:03}",
            "grid_zone": ZONES[(household - 1) % len(ZONES)],
            "event_time": at.isoformat(), "interval_seconds": INTERVAL_SECONDS,
            "consumption_kwh": round(demand_kw * INTERVAL_SECONDS / 3600, 6),
            "solar_generation_kwh": round(solar_kw * INTERVAL_SECONDS / 3600, 6)}


def validate(event):
    if not isinstance(event, dict):
        raise ValueError("Reading must be an object")
    for key in ("event_id", "meter_id", "household_id", "grid_zone", "event_time"):
        if not isinstance(event.get(key), str) or not event[key].strip():
            raise ValueError(f"Missing {key}")
    if event["grid_zone"] not in ZONES:
        raise ValueError("Unknown grid zone")
    dt = datetime.fromisoformat(event["event_time"].replace("Z", "+00:00"))
    if dt.tzinfo is None:
        raise ValueError("event_time must include a UTC offset")
    for key in ("interval_seconds", "consumption_kwh", "solar_generation_kwh"):
        value = event.get(key)
        if isinstance(value, bool) or not isinstance(value, (int, float)) or not math.isfinite(value):
            raise ValueError(f"Invalid {key}")
        if value < 0 or (key == "interval_seconds" and not 0 < value <= 86400):
            raise ValueError(f"Out-of-range {key}")
    # Date attribution assumes intervals do not cross midnight.
    at = dt.astimezone(timezone.utc)
    if (at + timedelta(seconds=event["interval_seconds"] - 0.000001)).date() != at.date():
        raise ValueError("Interval crosses a day boundary")
    return dt.astimezone(timezone.utc).replace(tzinfo=None)


def energy(consumption, solar):
    return {"solar_used_kwh": min(consumption, solar),
            "grid_import_kwh": max(consumption - solar, 0),
            "solar_export_kwh": max(solar - consumption, 0)}


def bill(import_kwh, tariff_rate, subsidy_flag):
    rate = Decimal(str(tariff_rate))
    if not rate.is_finite() or rate < 0:
        raise ValueError("Tariff must be finite and nonnegative")
    discount = Decimal("0.8") if subsidy_flag else Decimal("1")
    return float((Decimal(str(import_kwh)) * rate * discount).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP))
