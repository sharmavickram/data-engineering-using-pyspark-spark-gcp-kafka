from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import *


if __name__ == "__main__":
    spark = SparkSession.builder.appName("pivot-unpivot-in-spark-sql-pyspark").getOrCreate()

    studentDF = spark.read.option("header", "true").csv("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\students.csv")
    studentDF.printSchema()

    data_df = studentDF.withColumn("MARKS", studentDF["MARKS"].cast(IntegerType()))


    res1 = data_df.groupBy("ROLL_NO").pivot("SUBJECT").sum("MARKS")
    res1.printSchema()
    res1.show()