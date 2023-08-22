from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Joins-demo").getOrCreate()

    deptDF = spark.read.option("header", "true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/dept-updated.csv")
    empDF = spark.read.option("header", "true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/employees-updated.csv")

    print(".................Inner join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"],"inner").show()

    print(".................outer/full/fullouter join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"], "outer").show()

    print(".................Left/Leftouter join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"], "Leftouter").show()

    print(".................Right/Rightouter join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"], "rightouter").show()

    print(".................leftsemi  join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"], "leftsemi").show()

    print(".................leftanti join......................")
    empDF.join(deptDF, empDF["DEPARTMENT_ID"] == deptDF["DEPARTMENT_ID"], "leftanti").show()