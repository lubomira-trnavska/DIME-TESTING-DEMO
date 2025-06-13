from pyspark.sql.functions import col 
from pyspark.sql import SparkSession, Row

def filter_by_pattern(df,col_name,str_pattern):
    filter_df = df.filter(col(col_name).like(str_pattern))
    print(filter_df)
    return filter_df


def test_contains_pattern():
    spark = SparkSession.builder.appName("TestApp").getOrCreate()
    emp_data = [
        Row(emp_name="Mahesh", designation="Engineer", department="IT", salary=70000),
        Row(emp_name="Suresh", designation="Manager", department="HR", salary=90000),
        Row(emp_name="Naresh", designation="Analyst", department="Finance", salary=60000),
        Row(emp_name="Krishnan", designation="Engineer", department="IT", salary=75000),
        Row(emp_name="Mohan", designation="Technician", department="Support", salary=50000),
        Row(emp_name="Rajesh", designation="Engineer", department="R&D", salary=72000),
        Row(emp_name="Suresh", designation="Manager", department="Sales", salary=95000),
        Row(emp_name="Monika", designation="Lead", department="Design", salary=88000)
    ]
    emp_df = spark.createDataFrame(emp_data)
    result = filter_by_pattern(emp_df, "emp_name", "%an%")
    # result = filter_by_pattern(emp_df, "emp_name", "%esh%")
    result.show()
    names = [row.emp_name for row in result.collect()]
    # assert set(names) == {"Mahesh", "Suresh", "Naresh", "Rajesh"}
    
    assert set(names) == {"Krishnan","Mohan"}

# Run the test
test_contains_pattern()