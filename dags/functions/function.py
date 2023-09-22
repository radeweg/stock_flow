from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.functions import *

spark = SparkSession.builder.appName('airflow').getOrCreate()


def add_new_required_column(df):
    pysparkDF = df.withColumn('Datetime', date_format(to_date('date'), 'yyyy-MM-dd'))
    pysparkDF = pysparkDF.withColumn('year', date_format(to_date('date'), 'yyyy'))
    pysparkDF = pysparkDF.withColumn('month', date_format(to_date('date'), 'MM'))
    pysparkDF = pysparkDF.withColumn('day', date_format(to_date('date'), 'dd'))

    return pysparkDF


def moving_average(df):
    from pyspark.sql.window import Window

    window_spec = Window.orderBy(col("date"))
    result = df.withColumn('moving_average', avg("close").over(window_spec))
    return result


def total_return(df):
    df = df.withColumn('date', df['date'].cast('date'))
    df = df.orderBy('date')
    initial_close_price = df.select(first('close')).collect()[0][0]
    final_close_price = df.select(last('close')).collect()[0][0]
    total_return_price = (final_close_price - initial_close_price) / initial_close_price
    total_return_percentage = total_return_price * 100
    total_return_df = spark.createDataFrame([(total_return_percentage,)], ['total_return_percentage'])

    return total_return_df


def count_rent_day_by_day(df):
    result = (df.select("open").toPandas() - df.select("open").toPandas().shift(1)) / df.select(
        "open").toPandas().shift(1)
    return result


def count_ytd_return(df):
    result = df.groupBy("year").agg(
        ((F.expr("max_by(close, Datetime)") - F.expr("min_by(close, Datetime)")) /
         F.expr("min_by(close, Datetime)") * 100
         ).alias("YTD")
    )
    return result


def count_hv_ratio(df):
    result = df.withColumn("HV Ratio", df["high"] / df["volume"])

    return result
