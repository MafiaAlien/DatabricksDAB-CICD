from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")

# With an explicit schema, _rescued_data must be declared as well: it is where
# Auto Loader puts fields it fails to parse or that the schema does not cover
schema = StructType([
    StructField("ride_id", StringType(), True),
    StructField("rideable_type", StringType(), True),
    StructField("started_at", TimestampType(), True),
    StructField("ended_at", TimestampType(), True),
    StructField("start_station_name", StringType(), True),
    StructField("start_station_id", StringType(), True),
    StructField("end_station_name", StringType(), True),
    StructField("end_station_id", StringType(), True),
    StructField("start_lat", DecimalType(), True),
    StructField("start_lng", DecimalType(), True),
    StructField("end_lat", DecimalType(), True),
    StructField("end_lng", DecimalType(), True),
    StructField("member_casual", StringType(), True),
    StructField("_rescued_data", StringType(), True),
])


@dp.table(
    name=f"{bronze_schema}.jc_citibike",
    comment="Bronze layer: raw Citibike trip data, incrementally ingested from the landing volume",
)
def bronze_jc_citibike():
    # Point at the directory, not a single file, so new monthly CSVs dropped
    # into the volume are picked up on the next run without a code change
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(schema)
        .load(f"/Volumes/{catalog}/00_landing/source_citibike_data/")
    )
