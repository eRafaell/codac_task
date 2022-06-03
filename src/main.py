from typing import List
from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType

from functions import *

# dataset_one_path = "./input_datasets/dataset_one.csv"
# dataset_two_path = "./input_datasets/dataset_two.csv"
# filtered_list = ["United Kingdom", "Netherlands"]

def main(dataset1_path: str, dataset2_path: str, filtered_countries):
    
    spark_session = (SparkSession.builder.appName("spark_app").getOrCreate())

    df1_raw = read_csv(spark_session, dataset1_path)
    df2_raw = read_csv(spark_session, dataset2_path)

    df1 = df1_raw.alias("df1")
    df2 = df2_raw.alias("df2")

    df1 = (df1.withColumn("id", df1.id.cast(IntegerType())))
    df2 = (df2.withColumn("id", df2.id.cast(IntegerType())))
    
    df1.printSchema()

    df = join_dataframes(df1, df2, "id")
    df.printSchema()

    logger.info("dropping unneeded columns")
    df = drop_columns(df, ["first_name", "last_name", "cc_n"])
    
    column_rename = {"id": "client_identifier",
                     "btc_a": "bitcoin_address",
                     "cc_t": "credit_card_type"}
    
    df = rename_columns(df, column_rename)

    df = filter_df(df, "country", filtered_countries)
    df.show()
    print(df.count())

    save_df_to_csv(df)


if __name__ == "__main__":
    logger.info(f"Starting app")
    main(*parse_args())


    
