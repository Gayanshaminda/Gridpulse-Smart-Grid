# GridPulse
## Smart Grid Energy Monitoring and Daily Billing using a Lambda Architecture

EC8203 Applied Big Data Engineering Mini-Project

Implementation report draft | September 2026

Student names and IDs: complete before submission.

## Abstract

GridPulse is a small, complete data platform for monitoring household electricity demand and solar generation while reconciling daily energy imports against a separate tariff feed. Python simulators produce continuous meter events and daily CSV files. Kafka buffers the event stream, Spark Structured Streaming validates and transforms readings, and a separate Spark batch process rebuilds daily bills from archived Parquet data. Apache Airflow schedules the historical workflow. PostgreSQL serves a dashboard and read-only metrics API.

The design adopts Lambda architecture because low-latency operational visibility and repeatable financial reconciliation have different requirements. The live view is provisional, while completed-day bills are published as transactional snapshots. Stable event IDs, streaming checkpoints, atomic archive publication and repeatable writes support recovery without double charging.

## Business objective and scope

The platform answers two related questions: what is current demand and solar contribution by grid zone, and what does each household owe after its daily tariff is applied? The initial scenario contains 50 fictional households across three Colombo-labelled zones. It uses no real utility or household data.

All tariffs are simplified teaching values. The system excludes export payments, fixed charges, taxes, batteries and progressive tariff bands. This keeps the engineering work focused on two-source ingestion, processing, reconciliation and operational visibility.

The source code, reproducible container configuration, tests and demonstration guide accompany this report. Student details, actual individual contributions and a recorded or live demonstration must be completed by the submitting team.

<!-- page -->
# 1. Architecture decision

{{architecture}}

## Why Lambda fits

The speed layer makes recent readings available without waiting for a daily reference feed. The batch layer independently recomputes daily household totals from saved raw events, joins the tariff valid for that day and replaces the authoritative result. The serving layer exposes both outputs with distinct meanings, avoiding overlap or double counting.

Historical recalculation is valuable when a tariff is corrected, a processing rule changes or readings arrive late. A provisional dashboard can tolerate temporary incompleteness; an apparently final bill should not silently omit a household or tariff. These different consistency requirements justify two processing paths in this project.

## Rejected alternative: Kappa

A Kappa design could represent tariff updates as events and replay retained meter data through a single processing path. This reduces duplicated infrastructure and transformation logic. However, it introduces temporal reference-data joins, event-version semantics and coordination of replays with the serving results. Those concerns are possible to solve, but add complexity to this two-week scenario.

Lambda is not automatically superior merely because one input is a file. It is selected here for the explicit separation between fast visibility and repeatable historical billing. Its main drawback is maintaining equivalent rules in two paths. Shared validation and energy calculations, plus integration tests, reduce the risk of divergence without eliminating it.

## Deployment boundary

Kafka, PostgreSQL, Spark, Airflow, the simulators, API and dashboard run in local Docker services. Persistent named volumes retain broker data, raw archives, checkpoints, database state and task logs. Only the dashboard, API and Airflow web interface publish ports, bound to localhost.

<!-- page -->
# 2. Data sources and simulated time

## Continuous meter readings

Each event contains event_id, meter_id, household_id, grid_zone, event_time, interval_seconds, consumption_kwh and solar_generation_kwh. Consumption and solar generation are energy during an interval, not cumulative meter readings or instantaneous power. Household IDs are Kafka keys, so one household's messages remain ordered within a partition. The topic has three partitions and one replica for the laptop demonstration.

Demand follows reproducible morning and evening peaks. Solar follows a daytime profile; every third household has no solar. A dashboard switch simulates cloudy weather by reducing subsequent solar output. Stable IDs encode the simulation start date, tick and household. The producer preserves a pending tick's cloud setting before sending, so retrying that tick reproduces its payloads.

## Daily reference feed

A second Python process publishes one CSV per completed simulated day. Each row contains simulated_date, household_id, tariff_rate and subsidy_flag. The file is written to a temporary path and renamed only when complete. The batch job requires exactly one valid tariff row for every expected household. Negative or non-finite rates, duplicate households, incorrect dates and invalid subsidy flags are rejected.

## Clock contract

At the default settings, a producer emits one reading per household every two real seconds. One tick represents 576 simulated seconds; 150 ticks constitute one simulated day. Thus a day takes approximately 300 real seconds while the producer runs normally. The producer's acknowledged tick cursor is stored in PostgreSQL and is the shared source of truth for day completion.

Pausing, restarting or backpressure can extend this wall-clock duration. Other services never derive independent clocks. The simulation timestamps use UTC coordinates; they do not claim to represent local civil time in Sri Lanka. Clock configuration must not change in the middle of an existing simulation.

## Validation

Readings must contain nonempty identifiers, a known zone, an offset-aware timestamp, positive interval length and finite nonnegative energy values. Intervals crossing a date boundary are rejected because this simplified billing model attributes an interval wholly to one day. Invalid envelopes remain inspectable in a rejected-record table with a reason and Kafka source identifier.

<!-- page -->
# 3. Processing, billing and consistency

## Shared energy rules

Solar used at home = minimum(consumption, solar generation).

Grid import = maximum(consumption - solar generation, 0).

Solar export = maximum(solar generation - consumption, 0).

Daily bill = round(sum of interval grid imports x daily tariff x subsidy factor, 2).

The subsidy factor is 0.8 for eligible households and 1.0 otherwise. Imports are computed per interval before aggregation. For example, an interval consuming 1 kWh and generating 5 kWh imports zero; a later interval consuming 3 kWh with no solar imports 3 kWh. The bill must use 3 kWh, even though total solar generation exceeds total consumption across the two intervals.

## Speed layer

Spark Structured Streaming reads Kafka envelopes with a bounded number of offsets per microbatch. Raw envelopes are written to Parquet before serving changes. Completed directories are published atomically, so the batch reader avoids partially written archives. Spark parses valid JSON and derives the three energy measures. The PostgreSQL sink uses the event ID as a unique key. Retried callbacks and replayed source messages therefore do not create duplicate serving readings.

Live charts convert interval energy to average power using kWh x 3600 / interval_seconds. Solar contribution is self-consumed solar divided by household demand; exported generation is excluded from that measure. The latest snapshot states how many household readings have arrived.

## Authoritative batch layer

Airflow checks for completed simulated days every real minute. Spark rereads the raw archive, applies the shared validation, removes duplicate event IDs, groups by date/household/zone and joins the daily tariff. It verifies the expected household identities and interval counts before publishing. Monetary totals use decimal arithmetic and two-decimal rounding.

Airflow invokes the finite job through an internal HTTP worker using the same Spark image as the streaming service. This keeps the scheduler lightweight and avoids duplicate Spark installations. A lock prevents overlapping jobs, while timeout cleanup terminates the job and its JVM before a retry.

One database transaction replaces all bills for a day and marks its run complete. Repeating a day therefore replaces a snapshot rather than appending new charges. Automatic reconciliation catches up older unprocessed days and revisits the last three completed days, handling up to three dates per task. Corrections to older completed dates need an explicit backfill. A failed rerun retains an earlier successful snapshot while showing the failure and the snapshot timestamp.

<!-- page -->
# 4. Technology choices and observability

## Technology rationale

Kafka decouples simulated sources from processing and demonstrates partitions, keyed ordering and replay. Spark Structured Streaming provides DataFrame-based validation and energy transformations, while Spark batch uses the same processing family for historical aggregation and joins. Airflow provides finite-workflow scheduling, retries, task logs and an inspectable execution history.

Parquet preserves a compact historical source independent of the provisional database. PostgreSQL supplies unique constraints, SQL queries and atomic daily replacement. Streamlit exposes the business results with little UI infrastructure. FastAPI provides independently accessible live, billing, health and metrics endpoints. Docker Compose documents service dependencies and makes the stack reproducible on a local Linux-container engine.

## Logs and operational rules

Structured JSON logs record stage, event, microbatch or simulated-day identifiers and result counts. Rejected payloads retain their Kafka topic/partition/offset source identifier. The database stores producer progress, stream heartbeats, the latest microbatch duration and daily job outcomes. Airflow retains task failure and retry evidence.

The no-data rule fires when no valid reading has arrived for 30 real seconds. It uses wall-clock arrival timestamps, not compressed simulation time. A second rule flags a rejection ratio above 5% over the last 60 seconds. Incomplete daily billing jobs also appear as warnings. The separate renewable-contribution alert checks whether solar supplies less than 15% of demand during simulated daytime hours.

## Queryable monitoring outputs

The /health endpoint provides alert messages and latest batch states. The /metrics endpoint exports accepted/rejected counts, arrival age, error ratio and active-alert count in Prometheus text format. /api/live and /api/bills expose queryable business results. The dashboard's health page presents the same operational state alongside recent rejected readings.

## Recovery semantics

The system does not claim one distributed transaction across Kafka, the archive and PostgreSQL. It relies on retained source data, checkpoints and idempotent writes. Event IDs denote immutable readings. Conflicting revisions under an existing ID are outside the simulator contract. Checkpoints and their associated archives must be retained together; arbitrary partial state deletion is unsupported.

<!-- page -->
# 5. Dashboard: live energy

{{screenshot_live}}

{{screenshot_caption}}

The live page presents current demand, solar generation, grid imports and the fraction of demand supplied by self-consumed solar. Its time-series chart shows how daytime generation and morning/evening demand interact. Zone totals and high-demand households support operational inspection.

The simulation's cloudy-weather switch affects subsequent meter events, so its impact should pass through Kafka and Spark before appearing on the full-pipeline dashboard. It is most visible during simulated daytime. Nighttime naturally has low solar output and is excluded from the daytime business alert.

All live values are provisional. A partially arrived interval displays its household coverage rather than silently claiming a complete fleet snapshot. Refreshing a chart does not advance the simulation clock; the producer is responsible for that clock.

<!-- page -->
# 6. Dashboard: daily reconciliation

{{screenshot_billing}}

{{screenshot_caption}}

The daily page allows selection of a completed simulated date. It displays household count, grid energy, solar self-consumption and total estimated bills. The chart compares household bills, while the table includes the tariff and subsidy used. A CSV download makes the consolidated result available outside the dashboard.

The reconciliation timestamp identifies the snapshot's freshness. A corrected tariff followed by a successful rerun updates the existing day's bills. The live page and daily page are separate presentations of provisional and authoritative data; neither adds its values to the other.

For the teaching tariff, a household importing 10 kWh at LKR 35/kWh owes LKR 350 without subsidy or LKR 280 with the 20% subsidy. These figures illustrate the arithmetic, not an actual utility bill.

<!-- page -->
# 7. Verification and demonstration

{{validation}}

## Test strategy

Pure business-rule tests use small, independently calculable inputs. They verify energy conservation, surplus solar, import billing across separate intervals, subsidy and half-up rounding, invalid readings, reproducible generation and day boundaries. Store tests verify that retries preserve a single reading, invalid envelopes are quarantined once, daily reruns replace snapshots and stale arrivals trigger a warning.

Tariff tests cover duplicate or missing households, negative/non-finite rates, wrong dates and invalid flags. Spark integration tests create actual Parquet archives, apply validation and transformations, reconcile a day repeatedly and modify a tariff to verify historical recomputation. The dashboard is checked through both Streamlit's interaction harness and a headless browser.

## End-to-end acceptance criteria

A successful full-system check must observe Kafka events reaching Spark, archived data and PostgreSQL; live values changing; an Airflow task completing; one bill per household for a finished day; unchanged totals on an unchanged rerun; and a no-data alert followed by recovery after the producer resumes. A dashboard-only fixture cannot establish these properties.

## Demonstration sequence

Introduce the business question and architecture. Show live demand and solar, then a cloudy-weather change during daylight. Inspect a daily report and the Airflow task that produced it. Pause the meter source, wait for the backlog to drain and the freshness threshold to expire, then demonstrate the warning and recovery. Finish with a rerun and its unchanged row count/totals. The accompanying DEMO.md provides commands and timings for a five-to-ten-minute presentation.

<!-- page -->
# 8. Limitations, extensions and references

## Current limitations

This is an educational deployment with one Kafka broker, local Spark execution, a shared filesystem and one PostgreSQL instance. Kafka replication factor one does not tolerate broker loss. The serving path collects a bounded microbatch on the driver and inserts records individually, limiting throughput. Historical jobs scan the archive rather than pruning day partitions. No production-scale performance claim is made.

Daily coverage checks assume the deterministic simulator's immutable event identity and regular intervals. A production source would require exact expected-interval validation, explicit event revision rules and more complex boundary attribution. The last-three-day automatic replay window is a policy choice; older corrections require manual backfill. Different financial rules would require additional treatment of export credit, tariff tiers, fixed charges and taxes.

The metrics and correlation fields support basic diagnosis, but there is no full distributed tracing or external alert-delivery service. Local credentials are demonstration defaults. Production requires authentication, managed secrets, access controls, replicated storage and tested backups.

## Production extensions

Partition raw object storage by event date; distribute Spark; use scalable bulk/idempotent sink writes; replicate Kafka; introduce formal schemas and source contracts; monitor consumer lag and end-to-end latency; and define late-data and correction service levels. Benchmark these changes with realistic workloads before choosing operational capacity.

## References

1. EC8203 MiniProject 2026. Supplied assessment brief, five pages.

2. Apache Spark. Structured Streaming Programming Guide, version 3.5.7. https://spark.apache.org/docs/3.5.7/structured-streaming-programming-guide.html

3. Apache Spark. Structured Streaming + Kafka Integration Guide, version 3.5.7. https://spark.apache.org/docs/3.5.7/structured-streaming-kafka-integration.html

4. Apache Airflow. Running Airflow in Docker, version 2.10.5. https://airflow.apache.org/docs/apache-airflow/2.10.5/howto/docker-compose/index.html

5. Apache Kafka. Official Docker image documentation. https://hub.docker.com/r/apache/kafka

## Submission ownership

Add names, student IDs and a truthful statement of individual contributions. Review every core pipeline function and architectural decision before the viva. The submitting team should replace or supplement the included evidence with its final recorded demonstration and disclose assistance according to course rules.
