from pyspark import pipelines as dp
import pyspark.sql.functions as F

bronze_schema = spark.conf.get("bronze_schema")
silver_schema = spark.conf.get("silver_schema")


@dp.table(
    name=f"{silver_schema}.jc_citibike",
    comment="Silver layer: cleaned and enriched Citibike trip data",
)
@dp.expect_or_fail("valid_ride_id", "ride_id IS NOT NULL")
@dp.expect_or_drop("positive_duration", "trip_duration_mins > 0 AND trip_duration_mins IS NOT NULL")
@dp.expect("reasonable_duration", "trip_duration_mins < 1440")
def silver_jc_citibike():
    return (
        spark.readStream.table(f"{bronze_schema}.jc_citibike")
        .withColumn(
            "trip_duration_mins",
            (F.unix_timestamp(F.col("ended_at")) - F.unix_timestamp(F.col("started_at"))) / 60, # transfer unix time seconds to min
        )
        .withColumn("trip_start_date", F.to_date(F.col("started_at")))
        .select(
            "ride_id",
            "trip_start_date",
            "started_at",
            "ended_at",
            "start_station_name",
            "end_station_name",
            "trip_duration_mins",
        )
    )
