import json
import logging
from datetime import datetime, timezone

logging.basicConfig(level=logging.INFO, format="%(message)s")


def log(stage, event, **fields):
    logging.info(json.dumps({"time": datetime.now(timezone.utc).isoformat(),
                             "stage": stage, "event": event, **fields}, default=str))

