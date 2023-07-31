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
    ytd = f.count_ytd_return(pyspark_df)
    f.count_hv_ratio(pyspark_df)
    moving_average = f.moving_average(pyspark_df)

    # write
    ytd.select("*").write.mode("overwrite").format("jdbc") \
        .option("url", 'jdbc:postgresql://postgres:5432/postgres') \
        .option("driver", "org.postgresql.Driver") \
        .option("dbtable", "ytd_table") \
        .option("user", "airflow") \
        .option("password", "airflow").save()

    # read
    logging.info(spark.read.format("jdbc") \
                 .option("url", 'jdbc:postgresql://postgres:5432/postgres') \
                 .option("driver", "org.postgresql.Driver") \
                 .option("user", "airflow") \
                 .option("password", "airflow") \
                 .option("query", "SELECT * FROM ytd_table") \
                 .load().show())
    ytd.show()
    moving_average.show()


if __name__ == '__main__':
    main()
