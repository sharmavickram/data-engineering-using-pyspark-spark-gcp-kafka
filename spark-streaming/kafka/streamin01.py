from pyspark.sql import SparkSession

if __name__ == "__main__":
    scala_version = '2.12'
    spark_version = '3.1.2'
    # TODO: Ensure match above values match the correct versions
    packages = [
        f'org.apache.spark:spark-sql-kafka-0-10_{scala_version}:{spark_version}',
        'org.apache.kafka:kafka-clients:3.2.1'
    ]
    spark = SparkSession.builder \
        .master("local") \
        .appName("kafka-example") \
        .config("spark.jars.packages", ",".join(packages)) \
        .getOrCreate()

    df = spark.readStream\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:29092")\
        .option("subscribe", "dataedge")\
        .option("startingOffsets", "earliest")\
        .load()

    df.printSchema()

    personStringDF = df.selectExpr("CAST(value AS STRING)")

    personStringDF.writeStream\
        .format("console")\
        .outputMode("append")\
        .start()\
        .awaitTermination()
