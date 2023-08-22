from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Reading .dat files").getOrCreate()

    df = spark.read.\
        csv('D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/dat/ratings.dat', sep='::', schema='UserID int, MovieID int, Rating int, Timestamp long')

    df.show()
