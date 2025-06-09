from pyspark.sql import functions as F
def null_percentage(table_name: str, column_name: str) -> float:
    # Load the table as a DataFrame
    df = spark.table(table_name)

    # Total number of rows in the table
    total_count = df.count()

    if total_count == 0:
        return 0.0

    # Count the number of nulls in the specified column
    null_count = df.filter(F.col(column_name).isNull()).count()

    # Compute and return the percentage
    return (null_count / total_count) * 100