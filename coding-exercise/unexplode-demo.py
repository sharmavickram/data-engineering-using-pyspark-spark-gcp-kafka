from pyspark.sql import SparkSession
from pyspark.sql.functions import explode

if __name__ == "__main__":
    spark = SparkSession.builder.appName("explode-demo").getOrCreate()
    data = [
        (1, ['w1', 'w2', 'w3']),
        (2, ['w3', 'w4'])
    ]
    column = ["ID", "Words"]

    df = spark.createDataFrame(data, column)
    df.show()

    df.select(df["ID"], explode(df.Words).alias("word")).show()

    #df2 = df.groupBy(word).agg()