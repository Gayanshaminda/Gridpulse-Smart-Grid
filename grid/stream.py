"""Kafka -> immutable Parquet microbatches -> idempotent PostgreSQL serving sink."""
import os
import shutil
import time
from pyspark.sql import functions as F

from grid.config import BROKER, TOPIC, DATA
from grid.logging import log
from grid.spark_common import session, canonical
from grid.store import engine, init, ingest, put_state


def main():
    db = engine()
    init(db)
    spark = session("gridpulse-speed-layer")
    raw = (spark.readStream.format("kafka").option("kafka.bootstrap.servers", BROKER)
           .option("subscribe", TOPIC).option("startingOffsets", "earliest")
           .option("failOnDataLoss", "true").option("maxOffsetsPerTrigger", 5000).load()
           .select(F.concat_ws(":", "topic", F.col("partition").cast("string"), F.col("offset").cast("string")).alias("source_id"),
                   F.col("value").cast("string").alias("payload")))
    archive = DATA / "raw"
    archive.mkdir(parents=True, exist_ok=True)

    def consume(frame, batch_id):
        started = time.monotonic()
        frame.persist()
        try:
            # Spark checkpoints may retry this callback. Publishing a directory
            # atomically keeps the batch reader away from half-written files.
            target = archive / f"batch-{batch_id:012}"
            if not frame.isEmpty():
                if not target.exists():
                    temp = archive / f".pending-{batch_id:012}"
                    if temp.exists():
                        shutil.rmtree(temp)
                    frame.write.mode("overwrite").parquet(str(temp))
                    os.rename(temp, target)
                clean = canonical(frame)
                derived = {r.event_id: {name: r[name] for name in
                           ("solar_used_kwh", "grid_import_kwh", "solar_export_kwh")}
                           for r in clean.select("event_id", "solar_used_kwh", "grid_import_kwh", "solar_export_kwh").collect()}
                metrics = ingest(db, ((r.source_id, r.payload) for r in frame.toLocalIterator()), derived)
            else:
                metrics = ingest(db, [])
            elapsed = round(time.monotonic() - started, 3)
            with db.begin() as conn:
                put_state(conn, "last_microbatch_seconds", elapsed)
                put_state(conn, "last_microbatch_id", batch_id)
            log("stream", "microbatch_committed", batch_id=batch_id, duration_seconds=elapsed, **metrics)
        finally:
            frame.unpersist()

    query = (raw.writeStream.foreachBatch(consume).queryName("gridpulse")
             .option("checkpointLocation", str(DATA / "checkpoints" / "meters"))
             .trigger(processingTime="5 seconds").start())
    query.awaitTermination()


if __name__ == "__main__":
    main()

