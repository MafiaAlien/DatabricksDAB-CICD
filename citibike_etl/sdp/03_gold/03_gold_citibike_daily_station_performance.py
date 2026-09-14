from pyspark import pipelines as dp
import pyspark.sql.functions as F

silver_schema = spark.conf.get("silver_schema")
gold_schema = spark.conf.get("gold_schema")


@dp.materialized_view(
    name=f"{gold_schema}.daily_station_performance",
    comment="Gold layer: daily ride performance metrics per start station",
)
def gold_daily_station_performance():
    return spark.read.table(f"{silver_schema}.jc_citibike").groupBy(
        "trip_start_date", "start_station_name"
    ).agg(
        F.round(F.avg("trip_duration_mins"), 2).alias("avg_trip_duration_mins"),
        F.count(F.lit(1)).alias("total_trips"),
    )
