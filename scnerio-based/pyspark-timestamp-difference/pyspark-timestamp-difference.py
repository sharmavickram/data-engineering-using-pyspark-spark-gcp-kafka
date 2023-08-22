from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("pyspark-timestamp-difference").getOrCreate()

    dates = [("1", "2019-07-01 12:01:19.111"),
             ("2", "2019-06-24 12:01:19.222"),
             ("3", "2019-11-16 16:44:55.406"),
             ("4", "2019-11-16 16:50:59.406")
             ]

    df = spark.createDataFrame(data=dates, schema=["id","input_timestamp"])
    df.show(truncate=False)

    # Calculate Time difference in Seconds
    df2 = df.withColumn("from_timestamp", to_timestamp(col("input_timestamp"))) \
        .withColumn("end_timestamp", current_timestamp()) \
        .withColumn("DiffInSeconds", col("end_timestamp").cast("long") - col("from_timestamp").cast("long"))
    df2.show(truncate=False)