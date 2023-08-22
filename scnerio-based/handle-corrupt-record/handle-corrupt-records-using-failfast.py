from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

if __name__ == "__main__":
    spark = SparkSession.builder.appName("handle_corrupted_record").getOrCreate()

    schema = StructType([
        StructField("DEPARTMENT_ID", IntegerType(), True),
        StructField("DEPARTMENT_NAME", StringType(), True),
        StructField("MANAGER_ID", IntegerType(), True),
        StructField("LOCATION_ID", IntegerType(), True)
    ])
    try:
        dept_df = spark.read.option("mode", "FAILFAST")\
        .schema(schema)\
        .option("header", True)\
        .csv("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\dept.csv").cache()
    except Exception as e: print(e)


    dept_df.printSchema()
    dept_df.show(truncate=False)