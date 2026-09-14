from pyspark import pipelines as dp
from pyspark.sql.types import StructType, StructField, StringType, DecimalType, TimestampType

catalog = spark.conf.get("catalog")
bronze_schema = spark.conf.get("bronze_schema")

# 显式 schema 时必须带上 _rescued_data，否则 Auto Loader 无处安放解析失败的字段
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
    # 指向目录而非单个文件：以后往 volume 里丢新的月度 csv 会自动被接上
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "csv")
        .option("header", "true")
        .option("pathGlobFilter", "*.csv")
        .schema(schema)
        .load(f"/Volumes/{catalog}/00_landing/source_citibike_data/")
    )
