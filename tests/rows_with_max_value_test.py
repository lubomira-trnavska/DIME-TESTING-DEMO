from unittest import *
from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import StructType, StructField, StringType, DateType, DecimalType, IntegerType
from datetime import datetime
from decimal import Decimal
import re



spark = SparkSession.builder.appName("create_Dataframe").getOrCreate()

schema = StructType([
    StructField("ProductName", StringType(), True),
    StructField("Date_of_expiration", DateType(), True),
    StructField("Price", DecimalType(10,2), True),
    StructField("Quantity", IntegerType(), True),
])

data = [
    ("Product1", datetime.strptime("2027-01-01","%Y-%m-%d"), Decimal(15), 25),
    ("Product2", datetime.strptime("2027-02-01","%Y-%m-%d"), Decimal(15.0), 100),
    ("Product3", datetime.strptime("2027-03-01","%Y-%m-%d"), Decimal(10.0), 15),
    ("Product4", datetime.strptime("2028-01-01","%Y-%m-%d"), Decimal(7), 10),
    ("Product5", datetime.strptime("2028-02-01","%Y-%m-%d"), Decimal(15), 100),
    ("Product6", datetime.strptime("2028-01-15","%Y-%m-%d"), Decimal(14.0), 100),        
]


product_df = spark.createDataFrame(data, schema)
#product_df.printSchema()
#product_df.display()


class find_max_value(TestCase):
    
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.appName("max_row").getOrCreate()

    def test_rows_with_max_value(self,df: DataFrame, ignore_first_column: bool):
        columns = df.columns
        finding_max_value = []
        if ignore_first_column == True:
            finding_max_value.append('')
            for column in columns[1:]:
                finding_max_value.append(df.agg({column: "max"}).collect()[0][0])
        else:
            for column in columns:
                finding_max_value.append(df.agg({column: "max"}).collect()[0][0])

        i = 0
        while i < len(columns):
            max_value_df = rows_with_max_value(df, ignore_first_column).filter(col(columns[i]) == finding_max_value[i]).select("max_of")
            max_of_count = max_value_df.count()
            column_name_count = max_value_df.filter(col("max_of").contains(f"{columns[i]}")).count()
            print(f"max_of_count:{max_of_count}, column_name_count:{column_name_count}")
            self.assertEqual(max_of_count,column_name_count,"Test Failed")
            i += 1

    def test_max_value(self):
        #Test will run by providing dataframe and ignore_first_column
        self.test_rows_with_max_value(product_df, True)
        

    def suite():
        suite = TestSuite()
        suite.addTest(find_max_value('test_max_value'))
        return suite

if __name__ == "__main__":
    suite = find_max_value.suite()
    runner = TextTestRunner()
    runner.run(suite)
    