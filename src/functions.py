import logging
import os
from shutil import rmtree

from logging.handlers import RotatingFileHandler
from time import strftime
from pyspark.sql import SparkSession, DataFrame
from typing import List, Dict, Any
from pyspark.sql.functions import col


def create_rotating_log(logPath: str, size: int = 10000, backupCount: int = 5) -> RotatingFileHandler:
    """
    Creates a rotating log

    Args:
        logPath (str): Path to log file
        size (int): Maximum size of log file. If the size is exceeded a new log is created
        backupCount (int): Maximum number of log files. If it is exceeded the oldest one is removed
    Returns:
        logger: RotatingFileHandler object
    """
    logger = logging.getLogger("Rotating Log")
    logger.setLevel(logging.INFO)
    
    if (logger.hasHandlers()):
        logger.handlers.clear()

    # add a rotating handler
    handler = RotatingFileHandler(logPath, maxBytes=size, backupCount=backupCount)
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    return logger


log_file = "./Logs/KommatiPara.log"
logger = create_rotating_log(log_file)


def read_csv(spark_session: SparkSession, file_path: str) -> DataFrame:
    """
    Opens csv file from given path

    Args:
        spark_session (SparkSession): SparkSession which should be run
        file_path (str): Path to csv file
        
    Returns:
        df: PySpark DataFrame object
    """
    df = spark_session.read.option('header', True).csv(file_path)
    logger.info(f'Reading csv file from {file_path}')
    return df


def join_dataframes(df1: DataFrame, df2: DataFrame, key: str) -> DataFrame:
    """
    Joins two DataFrame on given key

    Args:
        df1 (DataFrame): Input Datframe 1
        df2 (DataFrame): Input Dataframe 2
        key: Join key
    Returns:
        DataFrame: joined DataFrame
    """
    logger.info(f"Joined two DataFrames on: {key}")
    return df1.join(df2, key)


def drop_columns(df: DataFrame, columns_to_drop: List) -> DataFrame:
    """
    Dropping selected columns

    Args:
        df (DataFrame): Input dataframe
        columns (List): List of columns to drop
    Returns:
        DataFrame: DataFrame with removed columns 
    """
    logger.info(f"Columns: {columns_to_drop} removed from DataFrame")
    return df.drop(*columns_to_drop)


def rename_columns(df: DataFrame, mapper: Dict[str, str]) -> DataFrame:
    """ 
    Renames column names of a DataFrame

    Args:
        df (DataFrame): Input DataFrame
        mapper (Dict): A dict mapping from the old column names to new names
    Returns:
        DataFrame: DataFrame with changed columns name
    """
    for old_column_name, new_column_name in mapper.items():
        logger.info(f"Column renaming from '{old_column_name}' to '{new_column_name}'")
        df = df.withColumnRenamed(old_column_name, new_column_name)
    return df


def filter_df(
    df: DataFrame, column_name: DataFrame.columns, value_to_filtered: List[str]
    ) -> DataFrame:
    """ 
    Filters DataFrame columns with given values

    Args:
        df (DataFrame): Input DataFrame
        column_name (DataFrame.columns): column in input Dataframe that will be filtered
        value_to_filtered: List of values to be filtered
    Returns:
        DataFrame: DataFrame with filtered rows
    """
    logger.info(f"Filtered with {value_to_filtered} values in '{column_name}' column")
    return df.where(col(column_name).isin(value_to_filtered))


def save_df_to_csv(df: DataFrame, target_path: str = "./client_data/output.csv"): 
    """ 
    Saves the dataframe to one csv file

    Args:
        df (DataFrame): Input DataFrame
        target_path (str): path where DataFrame as csv will be saved
    """
    # writing df as csv file to temporary folder
    temp = "temp"
    df.coalesce(1).write.csv(temp, mode="overwrite", header=True)
    
    # getting csv filename
    for file in os.listdir(temp):
        if file.endswith(".csv"):
            filename = f"{temp}/{file}"

    # copying csv file to cliend_data folder
    os.system(f"cp {filename} {target_path}") 

    # deleting the temporary folder with contained files 
    for file in os.listdir(temp):
        os.remove(os.path.join(temp, file))
    os.rmdir(temp)

    logger.info(f"Saved DataFrame file as {filename} to {target_path}")