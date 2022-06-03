from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from functions import *

# main function executes the whole process
def main(dataset1_path: str, dataset2_path: str, filtered_countries):
    # creating SparkSession
    spark_session = (SparkSession.builder.appName("spark_app").getOrCreate())

    # creating raw df from csv files to be able to revert to the original data at any time
    df1_raw = read_csv(spark_session, dataset1_path)
    df2_raw = read_csv(spark_session, dataset2_path)

    # copying raw DataFrames to new variables. This data will be transformed  
    df1 = df1_raw.alias("df1")
    df2 = df2_raw.alias("df2")
    
    # changing data type of 'id' columns in both DataFrames
    df1 = (df1.withColumn("id", df1.id.cast(IntegerType())))
    df2 = (df2.withColumn("id", df2.id.cast(IntegerType())))
    
    # joining 2 DataFrames
    df = join_dataframes(df1, df2, "id")

    # dropping unneeded columns
    df = drop_columns(df, ["first_name", "last_name", "cc_n"])
    
    # renaming columns based on column_rename dict
    column_rename = {"id": "client_identifier",
                     "btc_a": "bitcoin_address",
                     "cc_t": "credit_card_type"}
    
    df = rename_columns(df, column_rename)
    
    # filtering records in DataFrame
    df = filter_df(df, "country", filtered_countries)

    # saving DataFrame as csv in client_data folder
    save_df_to_csv(df)


if __name__ == "__main__":
    logger.info(f"Starting app")
    main(*parse_args())


    
