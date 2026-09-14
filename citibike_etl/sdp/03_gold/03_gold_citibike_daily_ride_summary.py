from pyspark import pipelines as dp
import pyspark.sql.functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


# Aggregations need a materialized view with a batch read: streaming tables are
# append-only and would not recompute the aggregate when upstream rows change
@dp.materialized_view(
    name=f"{gold_schema}.daily_ride_summary",
    comment="Gold layer: daily aggregates of ride durations and trip counts",
)
def gold_daily_ride_summary():
    return spark.read.table(f"{silver_schema}.jc_citibike").groupBy("trip_start_date").agg(
        F.round(F.max("trip_duration_mins"), 2).alias("max_trip_duration_mins"),
        F.round(F.min("trip_duration_mins"), 2).alias("min_trip_duration_mins"),
        F.round(F.avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        F.count(F.lit(1)).alias("total_trips"),
    )
