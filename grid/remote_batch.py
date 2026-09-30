"""Standard-library client used by the lightweight Airflow worker."""
import argparse
import json
import os
from urllib.request import Request, urlopen
from urllib.error import HTTPError


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--day")
    args = parser.parse_args()
    payload = {"day": args.day} if args.day else {}
    request = Request(os.getenv("BATCH_WORKER_URL", "http://batch-worker:8001") + "/reconcile",
                      data=json.dumps(payload).encode(), headers={"Content-Type": "application/json"})
    try:
        with urlopen(request, timeout=270) as response:
            result = json.load(response)
        print(result["output"])
    except HTTPError as exc:
        raise RuntimeError(exc.read().decode()) from exc


if __name__ == "__main__":
    main()
