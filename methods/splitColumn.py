from pyspark.sql import SparkSession
from pyspark.sql.functions import col, split, concat_ws, slice

# Step 1: Define the split_column function
def split_column(df, column_name):  
    words_array = split(col(column_name), " ")  # split() breaks the string into an array using space as delimiter
    first_word = words_array.getItem(0)         # getItem(0) picks the first element from the array (the first word)
    rest_words = concat_ws(" ", slice(words_array, 2, 100)) # slice() takes elements from position 2 onward (1-based index); concat_ws() joins them into a single string with space

    return (df.withColumn(column_name + "_first", first_word)  # Add a new column to store the first word
             .withColumn(column_name + "_rest", rest_words))   # Add another column to store the rest of the words


# Step 2: Start SparkSession
spark = SparkSession.builder.master("local[1]").appName("SplitColumnTest").getOrCreate()

# Step 3: Create input data
data = [("Virubhadra G Manvi",),("Pramoda Patila",),("Sachin Ramesh Tendulkar",),("Virat Kohli",),("Rohit",)]

df = spark.createDataFrame(data, ["name"])   # Define DataFrame with column name 'name'

# Step 4: Display original input
print("=== INPUT DATA ===")
df.show(truncate=False)

# Step 5: Apply the split_column function
result_df = split_column(df, "name")

# Step 6: Display output with split columns
print("=== OUTPUT DATA ===")
result_df.show(truncate=False)