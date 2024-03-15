from pyspark.sql import SparkSession
from pyspark.sql.functions import col

# Initialize a SparkSession
spark = SparkSession.builder.appName("stack_columns").getOrCreate()

# Assuming you have a DataFrame 'df' with columns 'A' and 'B'
# df = ...

# Select column 'A' and rename it for consistency
df_A = df.select(col("A").alias("StackedColumn"))

# Select column 'B' and rename it to match the schema of df_A
df_B = df.select(col("B").alias("StackedColumn"))

# Union the two DataFrames to stack the values
df_stacked = df_A.union(df_B)

# Show the result
df_stacked.show()

# Remember to stop the SparkSession when you're done
spark.stop()
