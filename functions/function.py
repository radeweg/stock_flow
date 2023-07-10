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
    return (df.select("close").toPandas().iloc[-1] / df.select("close").toPandas().iloc[0] - 1) * 100


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
