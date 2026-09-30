# Five-to-ten-minute demonstration

Start the stack before recording so image downloads and first-time Spark startup are not part of the presentation. Wait until at least one daily report is available. Do not describe the simulated readings as real smart-meter data.

## 0:00–1:00 — Business question and architecture

Show the architecture diagram in README. Explain the two input sources, Lambda speed/batch/serving layers, and why authoritative billing is rebuilt from archived readings. State that 300 real seconds of normal emission represents a simulated day. State that tariffs and subsidy rules are fictional simplifications.

## 1:00–2:30 — Live energy

Open http://localhost:8501. Show demand, solar generation, grid import, contribution and zone chart. Explain kW versus interval kWh. During simulated daylight, toggle cloudy weather. Show solar generation fall and grid imports rise. A low-contribution business alert should appear if the threshold is crossed. Turn clouds off afterwards.

## 2:30–4:00 — Daily integration

Open Daily billing. Select a completed day and inspect household consumption, imports, tariff, subsidy and bill. Download its CSV. Demonstrate with one household that daily bills use interval imports and dated tariffs. Explain why solar exports do not cancel nighttime imports. Point out the last reconciliation timestamp.

Open Airflow at http://localhost:8080 (admin/admin). Show the `gridpulse_daily_billing` DAG, successful task logs and the daily publication message. The DAG runs every minute to discover completed simulated days; it is not claiming one real minute is a simulated day.

## 4:00–5:30 — Operational failure and recovery

Run `docker compose stop meters`, keeping the dashboard open on Pipeline health. Allow existing Kafka backlog to drain and then wait 30 seconds after the last accepted arrival. Show the no-data warning. Run `docker compose start meters` and demonstrate recovery. Explain why freshness uses wall-clock arrival times, not simulated event timestamps.

## 5:30–7:00 — Repeatability and evidence

Record the daily row count and total, then trigger a rerun in Airflow. Once it completes, confirm the same day has one bill per household and unchanged totals, unless source tariffs/readings were deliberately corrected. Show the automated tests and explain at least one hand-calculated fixture.

## 7:00–8:00 — Limits

State the single-node limits: one Kafka broker, local Spark, shared disk, driver-side bounded serving writes, and demo credentials. Describe production improvements and why the current architecture is appropriate for this assessment. Each member should explain their own contribution and core code.

## Before submission

- Include the repository or ZIP, PDF report and video/live-demo evidence.
- Add student names, IDs and actual individual contributions. Do not invent contributions.
- Rehearse the two energy equations, Lambda/Kappa trade-off, retry behaviour and units.
- Capture screenshots of all three dashboard pages and a successful Airflow task.

