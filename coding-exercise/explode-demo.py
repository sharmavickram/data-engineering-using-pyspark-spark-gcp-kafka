from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

if __name__ == "__main__":
    spark = SparkSession.builder.appName("explode-demo").getOrCreate()

    data = [
        (1, ['w1', 'w2', 'w3','w5','w8','w0']),
        (2, ['w3', 'w4']),
        (3, ['w0'])
    ]
    column = ["ID", "Words"]

    df = spark.createDataFrame(data, column)
    df.show(truncate=False)

    df.select(df["ID"], explode(df.Words).alias("word")).show(truncate=False)

