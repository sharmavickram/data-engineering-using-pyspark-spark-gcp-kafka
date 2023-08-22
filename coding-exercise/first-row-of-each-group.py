from pyspark.sql import SparkSession,Row
from pyspark.sql.functions import row_number, col

from pyspark.sql.window import Window

spark = SparkSession.builder.appName('first').getOrCreate()

if __name__ == "__main__":

    #Prepare Data & DataFrame
    empdeptDF = spark.read.option("header","true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/employee-department.csv")
    empdeptDF.printSchema()
    empdeptDF.show(truncate=False)

    #Select First Row From every Group
    w2 = Window.partitionBy("department").orderBy(col("salary"))
    empdeptDF.withColumn("row", row_number().over(w2)) \
        .filter(col("row") == 3)\
        .drop("row").show(truncate=False)

    # ##using SQL
    #empdeptDF.createOrReplaceTempView("employee")
    #spark.sql("select * from (select *, row_number() OVER (PARTITION BY department ORDER BY salary) as rn FROM employee" ).show(truncate=False)