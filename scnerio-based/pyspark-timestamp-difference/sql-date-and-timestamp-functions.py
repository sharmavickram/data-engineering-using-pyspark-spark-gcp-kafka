from pyspark.sql import SparkSession
from pyspark.sql.functions import current_date, col, date_format

if __name__ == "__main__":
    spark = SparkSession.builder.appName("sql-date-and-timestamp-functions").getOrCreate()

    df = spark.createDataFrame([('vikram','01-01-1989',), ('anvi','09-11-2018',), ('anshu','20-04-1991',), ('sharma','10-01-2017',)], ['name','dob'])
    df.printSchema()
    df.show()

    # current_date()
    df.select(current_date().alias("current_date")).show(1)
    df2 = df.select(col("dob"), date_format(col("dob"), "MM-dd-yyyy").alias("date_format"))
    df2.show()