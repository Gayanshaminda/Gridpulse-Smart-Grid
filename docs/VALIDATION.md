# Verification record

Verified on 30 September 2026 on the local Docker deployment. These are observed results, not estimates from the offline dashboard fixture.

- Full container test suite: **31 passed in 72.12 seconds**, including real Spark and Parquet checks. See `output/test-results.txt`.
- End-to-end acceptance: **passed**, with 25,300 accepted readings and three successful scheduled Airflow runs observed. See `output/smoke_result.json`.
- First simulated day, 2026-01-01: **50 bills**, **150 readings per household**, **584.965618 kWh** grid imports and **LKR 19,667.74** total bills. The totals match separate decimal aggregation of the deterministic source readings, using rate 35 and the every-fifth-household subsidy.
- Unchanged-source rerun: all household financial fields and the row count remained unchanged.
- Outage recovery: pausing meters produced the no-data alert after the backlog drained; restarting meters resumed valid arrivals. Meters and scheduler were restored.
- Dashboard: all three interaction checks passed, and live pages rendered without browser errors. Captures are under `output/screenshots/`.

Run `scripts/test.ps1` and `python scripts/smoke_test.py` to repeat the checks. The acceptance script temporarily pauses this project's scheduler and meters, then restores them. It assumes the default 50-household configuration. Counts and simulated dates increase while the stack runs.

The PDF report remains a draft until student names, IDs and actual contributions are supplied. Rehearse or record the demonstration using `docs/DEMO.md` before submission.
