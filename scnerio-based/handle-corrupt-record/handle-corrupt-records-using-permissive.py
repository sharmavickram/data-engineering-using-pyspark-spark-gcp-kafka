from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, IntegerType, StringType

from pyspark.sql.functions import col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("handle_corrupted_record").getOrCreate()

    # schema = StructType([
    #     StructField("DEPARTMENT_ID", IntegerType(), True),
    #     StructField("DEPARTMENT_NAME", StringType(), True),
    #     StructField("MANAGER_ID", IntegerType(), True),
    #     StructField("LOCATION_ID", IntegerType(), True),
    #     StructField("_corrupt_record", StringType(), True)
    # ])
    # dept_df = spark.read.option("mode", "PERMISSIVE")\
    #     .schema(schema)\
    #     .option("header", True)\
    #     .option("columnNameOfCorruptRecord", "_corrupt_record")\
    #     .csv("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\dept.csv").cache()
    #
    # dept_df.printSchema()
    # dept_df.show()
    # dept_df.where(col("_corrupt_record").isNull()).drop("_corrupt_record").show()

    data = """{"a": 1, "b":2, "c":3}|{"a": 1, "b":2, "c":3}|{"a": 1, "b, "c":10}""".split('|')
    corruptDf = spark.read.option("mode", "PERMISSIVE")\
                          .option("columnNameOfCorruptRecord", "_corrupt_record")\
                          .json(spark.sparkContext.parallelize(data))
    corruptDf.printSchema()
    corruptDf.show()