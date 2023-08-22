from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("dataframe to internal-hive-table") \
        .config("spark.hadoop.hive.exec.dynamic.partition", "true") \
        .config("spark.hadoop.hive.exec.dynamic.partition.mode", "nonstrict") \
        .enableHiveSupport()\
        .getOrCreate()

    df = spark.read.option("header", "true").csv(
        "D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/sales_data_sample.csv")
    df.show()

    df.write.mode('overwrite').saveAsTable("salesData2")
