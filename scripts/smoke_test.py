"""End-to-end acceptance check. Temporarily stops only this project's sources/scheduler."""
import json
import subprocess
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]


def docker(*args, timeout=300):
    result = subprocess.run(["docker", "compose", *args], cwd=ROOT, capture_output=True,
                            text=True, timeout=timeout)
    if result.returncode:
        raise RuntimeError(result.stdout + result.stderr)
    return result.stdout


def fetch(path):
    with urlopen("http://127.0.0.1:8000" + path, timeout=20) as response:
        return json.load(response)


def wait_for(label, check, seconds):
    deadline = time.monotonic() + seconds
    last_message = 0
    while time.monotonic() < deadline:
        result = check()
        if result:
            print(label + ": passed", flush=True)
            return result
        if time.monotonic() - last_message > 30:
            print("Waiting for " + label.lower(), flush=True)
            last_message = time.monotonic()
        time.sleep(5)
    raise TimeoutError(label)


def financial_snapshot(rows):
    return sorted([{k: v for k, v in row.items() if k != "updated_at"} for row in rows],
                  key=lambda row: row["household_id"])


def main():
    wait_for("Live ingestion", lambda: fetch("/health")["accepted_total"] > 0, 240)
    rows = wait_for("Daily billing", lambda: fetch("/api/bills"), 900)
    day = min(row["simulated_date"] for row in rows)
    before = fetch("/api/bills?day=" + day)
    assert len(before) == 50, f"Expected 50 bills, found {len(before)}"
    def successful_runs():
        return int(docker("exec", "-T", "postgres", "psql", "-U", "grid", "-d", "airflow", "-Atc",
                          "SELECT count(*) FROM dag_run WHERE dag_id='gridpulse_daily_billing' AND state='success';").strip())
    successes = wait_for("Successful Airflow run", successful_runs, 300)
    scheduler_stopped = meters_stopped = False
    try:
        docker("stop", "airflow-scheduler")
        scheduler_stopped = True
        def batch_idle():
            output = docker("exec", "-T", "api", "python", "-c",
                            "import json,urllib.request; print(json.load(urllib.request.urlopen('http://batch-worker:8001/health'))['status'])")
            return output.strip() == "ready"
        wait_for("Batch worker ready", batch_idle, 300)
        docker("exec", "-T", "api", "python", "-m", "grid.remote_batch", "--day", day)
        after = fetch("/api/bills?day=" + day)
        assert financial_snapshot(before) == financial_snapshot(after), "Rerun changed unchanged-source totals"
        print("Idempotent daily rerun: passed", flush=True)
        docker("stop", "meters")
        meters_stopped = True
        def no_data():
            h = fetch("/health")
            return h if any("No valid meter readings" in alert for alert in h["alerts"]) else None
        stopped_health = wait_for("No-data alert", no_data, 240)
        stopped_count = stopped_health["accepted_total"]
        docker("start", "meters")
        meters_stopped = False
        def recovered():
            h = fetch("/health")
            return h if h["accepted_total"] > stopped_count and h["last_reading_age_seconds"] < 30 else None
        final_health = wait_for("Ingestion recovery", recovered, 180)
        result = {"checked_at": datetime.now(timezone.utc).isoformat(), "simulated_date": day,
                  "household_bills": len(after), "total_bill_lkr": round(sum(r["bill_lkr"] for r in after), 2),
                  "accepted_readings": final_health["accepted_total"], "successful_airflow_runs": int(successes),
                  "idempotent_rerun": True, "no_data_alert": True, "recovery": True}
        output = ROOT / "output" / "smoke_result.json"
        output.parent.mkdir(exist_ok=True)
        output.write_text(json.dumps(result, indent=2), encoding="utf-8")
        print(json.dumps(result, indent=2), flush=True)
    finally:
        if meters_stopped:
            docker("start", "meters")
        if scheduler_stopped:
            docker("start", "airflow-scheduler")


if __name__ == "__main__":
    main()
