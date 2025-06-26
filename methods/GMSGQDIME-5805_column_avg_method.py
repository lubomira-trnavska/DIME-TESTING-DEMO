from pyspark.sql.functions import avg
from pyspark.sql.types import NumericType, DoubleType, FloatType
 
def average_values(df):
    cols = [f.name for f in df.schema if isinstance(f.dataType, NumericType, DoubleType, FloatType)]
    if not cols:
        return {}
    return df.select([avg(c).alias(c) for c in cols]).first().asDict()

import unittest
from pyspark.sql import SparkSession
from column_avg_method import average_values

class TestAverageValues(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.spark = SparkSession.builder.master("local[1]").appName("TestAverageValues").getOrCreate()

    @classmethod
    def tearDownClass(cls):
        cls.spark.stop()

    def test_average_with_numeric_columns(self):
        df = self.spark.createDataFrame([
            (1, 2.0),
            (3, 4.0),
            (5, 6.0)
        ], ["int_col", "double_col"])
        result = average_values(df)
        expected = {"int_col": 3.0, "double_col": 4.0}
        self.assertEqual(result, expected)

    def test_average_with_no_numeric_columns(self):
        df = self.spark.createDataFrame([
            ("x",), ("y",), ("z",)
        ])
        result = average_values(df)
        self.assertEqual(result, {})

if __name__ == "__main__":
    unittest.main()