import json
from pyspark.sql import SparkSession, functions as F, types as T

from grid.domain import validate

SCHEMA = T.StructType([
    T.StructField(name, T.StringType()) for name in
    ("event_id", "meter_id", "household_id", "grid_zone", "event_time")
] + [T.StructField(name, T.DoubleType()) for name in
     ("interval_seconds", "consumption_kwh", "solar_generation_kwh")])


def session(name):
    spark = (SparkSession.builder.appName(name).master("local[2]")
             .config("spark.sql.session.timeZone", "UTC")
             .config("spark.sql.shuffle.partitions", "2")
             .config("spark.ui.enabled", "false").getOrCreate())
    spark.sparkContext.setLogLevel("WARN")
    return spark


@F.udf(T.BooleanType())
def valid_payload(payload):
    try:
        validate(json.loads(payload))
        return True
    except (ValueError, TypeError, KeyError, OverflowError):
        return False


def canonical(raw):
    """Shared validation and energy transformations for speed and batch layers."""
    df = raw.where(valid_payload("payload")).withColumn("reading", F.from_json("payload", SCHEMA))
    df = df.select("source_id", "payload", "reading.*")
    df = df.withColumn("event_time", F.to_timestamp("event_time"))
    return (df.withColumn("simulated_date", F.date_format("event_time", "yyyy-MM-dd"))
            .withColumn("solar_used_kwh", F.least("consumption_kwh", "solar_generation_kwh"))
            .withColumn("grid_import_kwh", F.greatest(F.col("consumption_kwh") - F.col("solar_generation_kwh"), F.lit(0.0)))
            .withColumn("solar_export_kwh", F.greatest(F.col("solar_generation_kwh") - F.col("consumption_kwh"), F.lit(0.0))))

