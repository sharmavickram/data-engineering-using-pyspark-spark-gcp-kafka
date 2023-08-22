from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("data-skew-in-pyspark").getOrCreate()

    try:
        myntraDF = spark.read.format("csv") \
            .option("header", "true") \
            .load("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\Airports2.csv")

    except IOError:
        print("please select proper file as input.......")
    import  pyspark.sql.functions as F
    print(myntraDF.groupBy(F.spark_partition_id()).count().show())

    # salt
    myntraDF = myntraDF.withColumn('salt', F.rand())
    myntraDF = myntraDF.repartition(8, 'salt')
    print(myntraDF.groupBy(F.spark_partition_id()).count().show())


