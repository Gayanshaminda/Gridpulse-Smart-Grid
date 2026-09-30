"""Internal finite-job endpoint: Airflow orchestrates, the shared Spark image computes."""
import subprocess
import os
import signal
import sys
import threading
from datetime import date
from fastapi import FastAPI, HTTPException
from pydantic import BaseModel

app = FastAPI(title="GridPulse internal batch worker")
job_lock = threading.Lock()


class ReconcileRequest(BaseModel):
    day: date | None = None


@app.get("/health")
def health():
    return {"status": "busy" if job_lock.locked() else "ready"}


@app.post("/reconcile")
def reconcile(request: ReconcileRequest):
    if not job_lock.acquire(blocking=False):
        raise HTTPException(409, "Another reconciliation is running")
    try:
        command = [sys.executable, "-m", "grid.batch"]
        if request.day:
            command.extend(["--day", request.day.isoformat()])
        process = subprocess.Popen(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                   text=True, start_new_session=True)
        try:
            stdout, stderr = process.communicate(timeout=240)
        except subprocess.TimeoutExpired:
            # Stop the JVM too, so a timed-out job cannot publish after a retry.
            if os.name == "posix":
                os.killpg(process.pid, signal.SIGKILL)
            else:
                process.kill()
            process.communicate()
            raise HTTPException(504, "Batch job exceeded four minutes")
        output = (stdout + stderr)[-20000:]
        print(output, flush=True)
        if process.returncode:
            raise HTTPException(500, output)
        return {"status": "complete", "output": output}
    finally:
        job_lock.release()
