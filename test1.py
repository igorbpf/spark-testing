import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from pyspark_test import assert_df_equality
from moto import mock_glue
import boto3
from glue_job import (
    create_spark_session,
    check_table_partition,
    validate_dataframe_columns,
    rename_columns,
    create_columns,
    select_columns
)

@pytest.fixture
def spark():
    return create_spark_session()

@pytest.fixture
def glue_context(spark):
    return glue.SparkContext(spark.sparkContext)

@mock_glue
def test_spark_session_creation(spark):
    assert isinstance(spark, SparkSession)

@mock_glue
def test_table_partition_existence(glue_context):
    # Assuming "TestDatabase" and "TestTable" are present in the Glue catalog
    partition_key = "date"
    partition_value = "20220101"
    assert check_table_partition(glue_context, "TestDatabase", "TestTable", partition_key, partition_value)

@mock_glue
def test_dataframe_columns_existence(spark):
    # Assuming "TestTable" has columns: ["id", "name", "age"]
    test_data = [(1, "John", 25), (2, "Jane", 30)]
    columns_to_check = ["id", "name", "age"]

    df = spark.createDataFrame(test_data, ["id", "name", "age"])
    assert validate_dataframe_columns(df, columns_to_check)

@mock_glue
def test_column_renaming(spark):
    test_data = [(1, "John", 25), (2, "Jane", 30)]
    df = spark.createDataFrame(test_data, ["id", "name", "age"])

    column_mapping = {"id": "user_id", "name": "user_name"}
    renamed_df = rename_columns(df, column_mapping)

    assert set(renamed_df.columns) == set(["user_id", "user_name", "age"])

@mock_glue
def test_column_creation(spark):
    test_data = [("John", 25), ("Jane", 30)]
    df = spark.createDataFrame(test_data, ["name", "age"])

    new_columns = {"id": 0, "gender": "unknown"}
    modified_df = create_columns(df, new_columns)

    assert set(modified_df.columns) == set(["name", "age", "id", "gender"])

@mock_glue
def test_columns_selection(spark):
    test_data = [(1, "John", 25), (2, "Jane", 30)]
    df = spark.createDataFrame(test_data, ["id", "name", "age"])

    selected_columns = ["id", "name"]
    selected_df = select_columns(df, selected_columns)

    expected_data = [(1, "John"), (2, "Jane")]
    expected_df = spark.createDataFrame(expected_data, ["id", "name"])

    assert_df_equality(selected_df, expected_df)
