# Architecture decisions

| Decision | Reason | Cost / alternative |
|---|---|---|
| Lambda | Live operational estimates and authoritative historical billing have different latency and correction needs | Two paths must stay consistent; Kappa reduces paths but adds replay/temporal-join complexity |
| Kafka keyed by household, three partitions | Ordered readings per household, buffered ingestion, replay, visible partitioning | Single replica lacks broker fault tolerance |
| Spark Structured Streaming | Uses Spark DataFrames for parsing, cleaning and energy calculations, alongside the historical Spark job | Microbatch startup/latency overhead; not a hard real-time controller |
| Airflow LocalExecutor | Finite daily workflows, retry state and visible task logs | Separate metadata/web/scheduler overhead |
| Internal Spark batch worker | Airflow invokes finite jobs through HTTP while streaming and batch reuse one image | One extra internal endpoint; lock and timeout cleanup prevent overlapping jobs |
| Raw Parquet before serving writes | Historical rebuild independent of the provisional serving database | Local volume is not resilient object storage |
| PostgreSQL | Queryable serving results, unique keys and transactional daily replacement | Per-event inserts and central storage limit throughput |
| Streamlit + FastAPI | Simple read-oriented dashboard and independently inspectable metrics | Not a multi-tenant authenticated product |
| Deterministic simulated events | Reproducible calculations and stable event identity on restart | More regular than real-world sensors |
| Per-interval import billing | Prevents solar exports incorrectly cancelling later imports | Export compensation and time-of-use tariffs need additional rules |
| Three-day automatic recomputation window | Bounded demonstration of late/corrected data | Older corrections need an explicit date backfill |

## Consistency contract

An event ID denotes an immutable meter reading. A retry emits identical content. The producer persists its pending tick/cloud setting before sending and advances its committed cursor after Kafka acknowledgements. If it crashes after delivery but before cursor advancement, it resends the same readings. The serving unique key and batch duplicate removal absorb that replay.

Raw Kafka envelopes are archived before serving updates. If the serving transaction fails, Spark retries the microbatch and reuses the completed archive directory. The daily batch reads only completed archive directories. A single database transaction replaces a day's bill rows and marks the run complete. Historical files, Kafka and PostgreSQL are not one atomic transaction, so the system intentionally relies on replay/idempotence.

## Simulated clock and completion

One tick is 576 simulated seconds at the default settings. All households share the same tick. A day is eligible once 150 ticks have been acknowledged to Kafka. Kafka delivery does not imply Spark has finished archiving them; the batch checks household coverage and retries if the stream is behind. The batch tariff source uses the same committed tick cursor and publishes a completed-day file via atomic rename.

## Reconciliation safety

Coverage requires the expected number of valid unique readings for every household. A malformed/missing tariff or incomplete meter data blocks publication. Existing successful bills are retained if a subsequent rerun fails, and the failure appears on the health page. Bills carry an update timestamp to make staleness visible.

## Remaining production work

Partition raw data by day, validate exact expected interval identities, handle conflicting event revisions explicitly, scale the sink away from driver-side collection, authenticate endpoints, manage secrets, add durable backups and Kafka replication, monitor consumer lag and end-to-end traces, and define contractual late-data cutoffs. None of those should be claimed as already implemented.
