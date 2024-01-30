from pyspark.sql import SparkSession
from pyspark.sql.functions import col

def create_spark_session(app_name="MyGlueJob"):
    spark = SparkSession.builder.appName(app_name).getOrCreate()
    return spark

def check_table_partition(glue_context, database, table, partition_key, partition_value):
    dynamic_frame = glue_context.create_dynamic_frame.from_catalog(
        database=database,
        table_name=table,
        additional_options={"partitionKeys": [partition_key]}
    )

    partitions = dynamic_frame.toDF().select(partition_key).distinct().rdd.flatMap(lambda x: x).collect()
    return partition_value in partitions

def validate_dataframe_columns(dataframe, expected_columns):
    actual_columns = dataframe.columns
    return set(expected_columns) == set(actual_columns)

def rename_columns(dataframe, column_mapping):
    for old_name, new_name in column_mapping.items():
        dataframe = dataframe.withColumnRenamed(old_name, new_name)
    return dataframe

def create_columns(dataframe, new_columns):
    for column_name, default_value in new_columns.items():
        dataframe = dataframe.withColumn(column_name, col(default_value))
    return dataframe

def select_columns(dataframe, selected_columns):
    return dataframe.select(*selected_columns)
