from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DecimalType, BooleanType, DateType
from pyspark.sql.functions import max as spark_max
from decimal import Decimal
from datetime import date

#METHOD
def max_values(df):
    result = {}
    for field in df.schema.fields:
        if isinstance(field.dataType, (IntegerType, DecimalType, DateType)) and field.name in ['year', 'price', 'release_date']:
            max_val = df.select(spark_max(field.name)).collect()[0][0]

            if isinstance(field.dataType, DateType) and max_val is not None:
                max_val = max_val.strftime("%d/%m/%Y")

            if isinstance(max_val, Decimal):
                max_val = float(max_val)

            result[field.name] = max_val
    return result

#TESTS

def test_max_values():
    # Sample test data
    data = [
        (2020, "Shampoo", Decimal("15.99"), True, date(2020, 5, 20)),
        (2021, "Conditioner", Decimal("18.50"), True, date(2021, 8, 15)),
        (2022, "Hair Gel", Decimal("12.00"), False, date(2022, 2, 10))
    ]

    schema = StructType([
        StructField("year", IntegerType(), True),
        StructField("product_name", StringType(), True),
        StructField("price", DecimalType(10, 2), True),
        StructField("in_production", BooleanType(), True),
        StructField("release_date", DateType(), True)
    ])

    df = spark.createDataFrame(data, schema=schema)
    

    expected = {
        "year": 2022,
        "price": 18.5,
        "release_date": "10/02/2022"
    }

    result = max_values(df)
    assert result == expected, f"❌ Test failed. Expected: {expected}, got: {result}"
    print("✅ Unit test passed!")

#CALL THE TEST
test_max_values()
