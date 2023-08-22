from pyspark.sql import SparkSession
from pyspark.sql.functions import when, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("replace emptyvalues in dataframe").getOrCreate()

    df = spark.read.option("header", "true").csv("/D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/sample.csv")

    df.printSchema()
    df.withColumn("DEPARTMENT_NAME", \
                  when(col("DEPARTMENT_NAME") == "", None) \
                  .otherwise(col("DEPARTMENT_NAME"))) \
        .show()

    #Replace Empty Value with None on All DataFrame Columns
    df.select([when(col(c)=="",None).otherwise (col(c)).alias(c) for c in df.columns]).show()

    df2 = df.select([when(col(c) == "", "*").otherwise(col(c)).alias(c) for c in ["DEPARTMENT_NAME","MANAGER_ID","LOCATION_ID"]]).show()