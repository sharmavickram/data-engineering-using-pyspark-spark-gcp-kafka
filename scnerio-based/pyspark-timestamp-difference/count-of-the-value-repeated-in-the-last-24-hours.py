from pyspark.sql import functions as F
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession.builder.appName("count-of-the-value-repeated-in-the-last-24-hours").getOrCreate()

    df = spark.read.option("header", "true").csv(
        "D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\date-time.csv")

    df.withColumn('ip_count', F.expr("count(ip) over (partition by ip order by datetimecol range between interval 24 hours preceding and current row)")) \
              .withColumn('ip_count', when(F.col('ip') == 0, 0).otherwise(F.col('ip') - 1)).show()

#https://stackoverflow.com/questions/72183804/get-count-of-the-value-repeated-in-the-last-24-hours-in-pyspark-dataframe?rq=1