import requests
import functions.function as f
from pyspark.sql import SparkSession


def main():
    total_return_path = "hdfs://namenode:8020/spark_data_tr/"

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
    total_return = f.total_return(pyspark_df)


    # write
    total_return.write.parquet(total_return_path,mode="overwrite" )

    # read
    spark.read.parquet(total_return_path).show()


if __name__ == '__main__':
    main()
