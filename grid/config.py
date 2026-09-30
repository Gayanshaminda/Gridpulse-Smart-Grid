import os
from pathlib import Path

DATA = Path(os.getenv("DATA_DIR", "data"))
DATABASE_URL = os.getenv("DATABASE_URL", "sqlite:///data/grid.db")
BROKER = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")
TOPIC = "meter-readings"
HOUSEHOLDS = int(os.getenv("HOUSEHOLDS", "50"))
DAY_SECONDS = int(os.getenv("SIM_DAY_SECONDS", "300"))
EMIT_SECONDS = int(os.getenv("EMIT_SECONDS", "2"))
START_DATE = os.getenv("SIM_START_DATE", "2026-01-01")
ZONES = ("Colombo North", "Colombo Central", "Colombo South")
if HOUSEHOLDS < 1 or EMIT_SECONDS < 1 or DAY_SECONDS < EMIT_SECONDS or DAY_SECONDS % EMIT_SECONDS:
    raise ValueError("Use positive HOUSEHOLDS/EMIT_SECONDS; SIM_DAY_SECONDS must divide by EMIT_SECONDS")
TICKS_PER_DAY = DAY_SECONDS // EMIT_SECONDS
INTERVAL_SECONDS = 86400 / TICKS_PER_DAY
