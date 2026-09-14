# test_citibike_utils.py

import datetime
from citibike.citibike_utils import get_trip_duration_mins

def test_get_trip_duration_mins(spark):
    # Create a test DataFrame with known start and end timestamps using datetime objects
    data = [
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 10, 0)),  # 10 minutes
        (datetime.datetime(2025, 4, 10, 10, 0, 0), datetime.datetime(2025, 4, 10, 10, 30, 0))   # 30 minutes
    ]
    schema = "start_timestamp timestamp, end_timestamp timestamp"
    df = spark.createDataFrame(data, schema=schema)
    
    # Apply the function to calculate trip duration in minutes
    result_df = get_trip_duration_mins(spark, df, "start_timestamp", "end_timestamp", "trip_duration_mins")
    
    # Collect the results for assertions
    results = result_df.select("trip_duration_mins").collect()
    
    # Assert that the differences are as expected
    assert results[0]["trip_duration_mins"] == 10
    assert results[1]["trip_duration_mins"] == 30