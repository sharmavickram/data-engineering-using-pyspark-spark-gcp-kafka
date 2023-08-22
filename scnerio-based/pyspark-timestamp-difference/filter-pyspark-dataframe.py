from pyspark.sql import SparkSession
from pyspark.sql import functions as F, types as T

if __name__ == "__main__":
    spark = SparkSession.builder.appName("filter-pyspark-dataframe").getOrCreate()

    df = spark.createDataFrame(
        [
            ('2022-03-10',),
            ('2022-03-09',),
            ('2022-03-08',),
            ('2022-02-02',),
            ('2022-02-01',)
        ], ['Date']
    ).withColumn('Date', F.to_date('Date', 'yyyy-MM-dd'))

    df \
        .filter((F.col('Date') > F.date_sub(F.current_date(), 14))) \
        .show()

    #https://stackoverflow.com/questions/71429643/how-to-filter-pyspark-dataframe-with-last-14-days?rq=1