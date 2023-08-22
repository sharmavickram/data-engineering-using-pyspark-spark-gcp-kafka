from pyspark.sql import SparkSession
from pyspark.sql.types import IntegerType
import pyspark.sql.functions as sf
from pyspark.sql import Window

if __name__ == "__main__":
    spark = SparkSession.builder.appName("cumulative-sum-in-spark").getOrCreate()

    deptDF = spark.read \
        .option("header", "true") \
        .csv("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\scnerio-based\spark-cumulative-sum\data\dept.csv")
    deptDF.printSchema()
    deptDF.show()
    data_df = deptDF.withColumn("salary", deptDF['salary'].cast(IntegerType()))
    data_df.printSchema()

    win = Window.partitionBy('dept_name').orderBy('prof_id')
    cum_sum = data_df.withColumn('cumsum', sf.sum(data_df.salary).over(win))
    cum_sum.show()
