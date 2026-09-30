import subprocess
import pytest
from fastapi import HTTPException
import grid.batch_service as service


@pytest.mark.parametrize("exit_code,expected_status", [(0, None), (1, 500)])
def test_job_completion_and_lock_release(monkeypatch, exit_code, expected_status):
    commands = []
    class Process:
        returncode = exit_code
        def __init__(self, command, **kwargs): commands.append(command)
        def communicate(self, **kwargs): return ("job output", "")
    monkeypatch.setattr(service.subprocess, "Popen", Process)
    request = service.ReconcileRequest(day="2026-01-01")
    if expected_status:
        with pytest.raises(HTTPException) as error:
            service.reconcile(request)
        assert error.value.status_code == expected_status
    else:
        assert service.reconcile(request)["status"] == "complete"
    assert commands[0][-2:] == ["--day", "2026-01-01"]
    assert not service.job_lock.locked()


def test_overlapping_job_rejected():
    service.job_lock.acquire()
    try:
        with pytest.raises(HTTPException) as error:
            service.reconcile(service.ReconcileRequest())
        assert error.value.status_code == 409
    finally:
        service.job_lock.release()


def test_timeout_stops_process_and_releases_lock(monkeypatch):
    killed = []
    class Process:
        pid = 123
        def __init__(self, *args, **kwargs): self.calls = 0
        def communicate(self, **kwargs):
            self.calls += 1
            if self.calls == 1: raise subprocess.TimeoutExpired("batch", 240)
            return ("", "")
        def kill(self): killed.append(self.pid)
    monkeypatch.setattr(service.subprocess, "Popen", Process)
    monkeypatch.setattr(service.os, "killpg", lambda pid, sig: killed.append(pid), raising=False)
    with pytest.raises(HTTPException) as error:
        service.reconcile(service.ReconcileRequest())
    assert error.value.status_code == 504
    assert killed == [123]
    assert not service.job_lock.locked()
