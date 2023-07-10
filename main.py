import os
import logging
import requests
import functions.function as f
from pyspark.sql import SparkSession


def main():
    spark = (SparkSession.builder
             .getOrCreate())

    headers = {
        'Content-Type': 'application/json'
    }
    response = requests.get(
        "https://api.tiingo.com/iex/spy/prices?startDate=2016-12-31&resampleFreq=120min&columns=open,high,low,close,volume&token=ffb142c23913cd9f14d63954cf8ee8c062186a8f",
        headers=headers)

    data = response.json()

    pyspark_df = spark.createDataFrame(data)
    pyspark_df.describe().show()
    pyspark_df = f.add_new_required_column(pyspark_df)
    f.count_ytd_return(pyspark_df)
    f.count_hv_ratio(pyspark_df)
    f.moving_average(pyspark_df)

    # write
    pyspark_df.select("*").write.mode("overwrite").format("jdbc") \
        .option("url", 'jdbc:postgresql://localhost:5432/postgres') \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "postgres") \
        .option("user", "postgres") \
        .option("password", "postgres").save()

    # read
    logging.info(spark.read.format("jdbc") \
                 .option("url", 'jdbc:postgresql://localhost:5432/postgres') \
                 .option("driver", "org.postgresql.Driver") \
                 .option("user", "postgres") \
                 .option("password", "postgres") \
                 .option("query", "SELECT * FROM postgres") \
                 .load().show())


if __name__ == '__main__':
    main()
