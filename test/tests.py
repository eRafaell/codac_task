import pytest
import os
from chispa import assert_df_equality
from pyspark.sql import SparkSession
from src.functions import read_csv, rename_columns, drop_columns, filter_df, join_dataframes


@pytest.fixture(scope="session")
def spark():
    return SparkSession.builder.master("local").appName("chispa").getOrCreate()


def test_check_read_csv_correctly(spark):
    path_to_csv = "test/dataset_test.csv"
    source_df = read_csv(spark, path_to_csv)
    expected_count_df = source_df.count()
    assert 1000 == expected_count_df


def test_check_if_file_exists(spark):
    path_to_file = "test/dataset_test.csv"
    assert os.path.exists(path_to_file) == True


def test_column_drop_two_column(spark):
    source_data = [("a1", "a2", "a3", "a4"), ("b1", "b2", "b3", "b4"), ("c1", "c2", "c3", "c4")]
    expected_data = [("a1", "a2"), ("b1", "b2"), ("c1", "c2")]
    source_df = spark.createDataFrame(source_data, ["col1", "col2", "col3", "col4"])
    expected_df = spark.createDataFrame(expected_data, ["col1", "col2"])
    actual_df = drop_columns(source_df, ["col3", "col4"])
    assert_df_equality(actual_df, expected_df)


def test_rename_many_column(spark):
    source_data  = [("a", "b"), ("c", "d"), ("e", "f")]
    actual_col_names = ["col1", "col2"]
    expected_col_names = ["renamed_col1", "renamed_col2"]
    actual_df = spark.createDataFrame(source_data , actual_col_names)
    expected_df = spark.createDataFrame(source_data , expected_col_names)
    column_rename = {"col1": "renamed_col1", "col2": "renamed_col2"}
    assert_df_equality(
        rename_columns(actual_df, column_rename), expected_df, ignore_column_order=True
    )


def test_filter(spark):
    source_df = spark.createDataFrame([("a1", "a2"), ("b1", "b2"), ("c1", "c2")], ["col1", "col2"])
    expected_df = spark.createDataFrame([("c1", "c2")], ["col1", "col2"])
    actual_df = filter_df(source_df, "col1", ["c1"])
    assert_df_equality(actual_df, expected_df)


def test_join_two_dataframes(spark):
    df1 = spark.createDataFrame([(1, "a1"), (2, "b1"), (3, "c1")], ["id", "col1"])
    df2 = spark.createDataFrame([(1, "a2", "a3"), (2, "b2", "b3"), (3, "c2", "c3")], ["id", "col2", "col3"])
    expected_data = [(1, "a1", "a2", "a3"), (2, "b1", "b2", "b3"), (3, "c1", "c2", "c3")]
    expected_df = spark.createDataFrame(expected_data , ["id", "col1", "col2", "col3"])
    actual_df = join_dataframes(df1, df2, "id")
    assert_df_equality(actual_df, expected_df)