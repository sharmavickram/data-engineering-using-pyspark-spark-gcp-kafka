from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("LEFT Join-demo").getOrCreate()

    deptDF = spark.read.option("header", "true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/dept-updated.csv")
    empDF = spark.read.option("header", "true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/employees-updated.csv")

    deptDF.printSchema()
    empDF.printSchema()

    empDF.join(deptDF,empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"],"left").show()

