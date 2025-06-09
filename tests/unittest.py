from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

# Create test data
data = [
    (1, "A"),
    (2, None),
    (3, "B"),
    (4, None),
    (5, "C")
]

schema = StructType([
    StructField("ID", IntegerType(), True),
    StructField("ROW_KEY", StringType(), True)
])

df = spark.createDataFrame(data, schema)
df.createOrReplaceTempView("test_data")

table_name = "test_data"
column_name = "ROW_KEY"

null_percent = df.select(F.mean(F.col(column_name).isNull().cast('int')).alias('null_percentage')).collect()[0]['null_percentage']
display(null_percent)