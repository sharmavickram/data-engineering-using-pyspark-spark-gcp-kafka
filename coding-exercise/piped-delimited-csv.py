from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("piped-delimited-csv").getOrCreate()

    df = spark.read.option("header", "true").csv(
        "/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/piped.csv")
    df.show(truncate=False)
