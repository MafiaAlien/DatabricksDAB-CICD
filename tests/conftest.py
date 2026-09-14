import sys, os
import pytest

sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", "src"))

@pytest.fixture()
def spark():
    try:
        from databricks.connect import DatabricksSession
        return DatabricksSession.builder.getOrCreate()
    except ImportError:
        pass
    try:
        from pyspark.sql import SparkSession
        return SparkSession.builder.master("local[*]").getOrCreate()
    except ImportError as e:
        raise ImportError("Neither Databricks Connect nor PySpark is available") from e
