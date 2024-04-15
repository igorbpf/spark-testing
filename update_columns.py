from pyspark.sql import DataFrame
from pyspark.sql.functions import col, when

def update_columns_based_on_condition(df: DataFrame, check_columns: list, update_columns: list) -> DataFrame:
    """
    Update specific columns in a DataFrame based on the nullity of other columns.

    Parameters:
    df (DataFrame): The input DataFrame.
    check_columns (list): A list of two column names to check for null values.
    update_columns (list): A list of two column names to conditionally update based on the check.

    Returns:
    DataFrame: The updated DataFrame.
    """
    # Check that the lists have exactly two elements as expected
    assert len(check_columns) == 2, "check_columns list must contain exactly two column names"
    assert len(update_columns) == 2, "update_columns list must contain exactly two column names"

    # Build the condition for both columns being null
    condition = col(check_columns[0]).isNull() & col(check_columns[1]).isNull()

    # Update each of the target columns based on the condition
    for column in update_columns:
        df = df.withColumn(column, when(condition, None).otherwise(col(column)))

    return df


import pytest
from pyspark.sql import SparkSession
from pyspark.sql.functions import col
from your_module import update_columns_based_on_condition  # Import the function from your module

@pytest.fixture(scope="module")
def spark():
    """ Pytest fixture for creating a Spark session. """
    spark = SparkSession.builder.master("local[1]").appName("PySparkTest").getOrCreate()
    yield spark
    spark.stop()

def test_update_columns_all_none(spark):
    # Data where the check columns are all None
    data = [(None, None, 5, 6)]
    columns = ["A", "B", "C", "D"]
    df = spark.createDataFrame(data, columns)

    # Expected output data
    expected_data = [(None, None, None, None)]
    expected_df = spark.createDataFrame(expected_data, columns)

    # Apply the function
    updated_df = update_columns_based_on_condition(df, ["A", "B"], ["C", "D"])

    # Assert
    assert updated_df.collect() == expected_df.collect()

def test_update_columns_not_all_none(spark):
    # Data where not all check columns are None
    data = [(1, None, 5, 6)]
    columns = ["A", "B", "C", "D"]
    df = spark.createDataFrame(data, columns)

    # Expected output data
    expected_data = [(1, None, 5, 6)]
    expected_df = spark.createDataFrame(expected_data, columns)

    # Apply the function
    updated_df = update_columns_based_on_condition(df, ["A", "B"], ["C", "D"])

    # Assert
    assert updated_df.collect() == expected_df.collect()

def test_update_columns_empty_df(spark):
    # Empty DataFrame
    data = []
    columns = ["A", "B", "C", "D"]
    df = spark.createDataFrame(data, columns)

    # Expected output data
    expected_df = spark.createDataFrame(data, columns)

    # Apply the function
    updated_df = update_columns_based_on_condition(df, ["A", "B"], ["C", "D"])

    # Assert
    assert updated_df.collect() == expected_df.collect()

def test_update_columns_invalid_input(spark):
    # Data setup
    data = [(1, 2, 5, 6)]
    columns = ["A", "B", "C", "D"]
    df = spark.createDataFrame(data, columns)

    # Test with incorrect column names to check assertion errors
    with pytest.raises(AssertionError):
        update_columns_based_on_condition(df, ["A"], ["C", "D"])

    with pytest.raises(AssertionError):
        update_columns_based_on_condition(df, ["A", "B"], ["C"])
