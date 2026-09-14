from pyspark import pipelines as dp
import pyspark.sql.functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


# 聚合必须用 materialized_view + 批读：streaming table 是 append-only，
# 上游行变化时不会重算聚合结果
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
