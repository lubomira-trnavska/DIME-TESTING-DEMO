from pyspark.sql import DataFrame
from pyspark.sql.functions import concat, lit, col, monotonically_increasing_id,concat_ws, when

def rows_with_max_value(df: DataFrame, ignore_first_column: bool):
    """ 
    finding out the max value by iterating through each columns 
    and comparing each row of the column with identified max value 
    and appending the max column name to the 'max_of'
    """
    columns = df.columns
    row_value = df.collect()
    max_values = []
    df = df.coalesce(1).withColumn("index", monotonically_increasing_id()) #adding the index to the dataframe

    i = 0
    if ignore_first_column == True: #ignoring the first column
        i = 1
    while i < len(columns): 
        if len(max_values) != i: 
            max_values.append('')
            maximum = df.agg({columns[i]: "max"}).collect()[0][0]
            max_values.append(maximum) 
        else:
            maximum = df.agg({columns[i]: "max"}).collect()[0][0]
            max_values.append(maximum)
        j = 0
        while j < len(row_value): 
            if (row_value[j].__getitem__(columns[i]) == max_values[i]):
                df = df.withColumn(f"max_of_{columns[i]}_{j}", when(col("index") == j, lit(columns[i])).otherwise(lit(None)))
            j += 1
        i += 1
    col_list = [c for c in df.columns if c.startswith("max_of_")]
    df = df.withColumn("max_of", concat_ws(",", *col_list)).drop(*col_list, "index").filter(col("max_of")!='')
    return df