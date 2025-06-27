# Databricks notebook source
# DBTITLE 1,Data_frame_Subset columns
# Step 1: Import required modules from PySpark
from pyspark.sql import SparkSession, Row                   
from pyspark.sql.types import StructType, StructField, IntegerType, StringType  # For defining schema

# Step 2: Created a Spark session
spark = SparkSession.builder.master("local[*]").appName("EmployeeExample").getOrCreate()                                          

# Step 3: Defined data using `Row` objects
data = [
    Row(emp_id=101, name="Praveen", dept="IT", salary=70000, phone=None),
    Row(emp_id=102, name="Kiran", dept="HR", salary=80000, phone=None),
    Row(emp_id=103, name="Vinod", dept=None, salary=None, phone=None),
    Row(emp_id=104, name="Deepak", dept="IT", salary=90000, phone=None),
]

# Step 4: Defined schema using StructType and StructField
schema = StructType([
    StructField("emp_id", IntegerType(), True),     # True means the column can have null values
    StructField("name", StringType(), True),
    StructField("dept", StringType(), True),
    StructField("salary", IntegerType(), True),
    StructField("phone", StringType(), True)
])

# Step 5: Created a DataFrame using the data and schema
df = spark.createDataFrame(data, schema)  # This combines data with schema to create a structured DataFrame

# Step 6:Selected specific columns from a DataFrame
def select_columns(df, columns):
    return df.select(columns)             # .select() is used to choose specific columns

# Step 7: Call the function and selected all columns
result_df = select_columns(df, ['emp_id', 'name', 'dept', 'salary', 'phone'])

# Step 8: Display the resulting DataFrame
print("Result DataFrame")
result_df.show()   # `display()` works in Databricks; in general use `.show()` to print a DataFrame

# --------------------------------------------------------------

# Step 9: Drop columns where all values are NULL

# To do this, we need these additional imports:
from pyspark.sql.functions import col, sum, when

# Step 10: Define the function to drop columns where all rows have nulls
def drop_all_null_columns(df):
    # Step 10.1: For each column, count how many non-null entries exist
    null_sums = df.select([
        sum(when(col(c).isNotNull(), 1).otherwise(0)).alias(c)
        for c in df.columns
    ])
    # Explanation:
    # - col(c).isNotNull() returns True if the column value is not null
    # - when(..., 1).otherwise(0) gives 1 for non-null, 0 for null
    # - sum(...) aggregates the non-null count for each column
    # - alias(c) gives the result the same name as the original column

    # Step 10.2: Collect the results to the driver
    counts = null_sums.collect()[0]  # Get the first Row with non-null counts
    display(counts)

    # Step 10.3: Filter out columns with zero non-null values
    non_null_cols = [c for c in df.columns if counts[c] > 0]
    # For example: if 'phone' column has all None, then counts['phone'] will be 0 and dropped

    # Step 10.4: Return a new DataFrame containing only non-null columns
    return df.select(non_null_cols)

print("Non-Null Columns DataFrame")

# Step 11: Apply the function to drop columns with all nulls
drop_all_null_columns(df).show()  # Display the cleaned DataFrame
