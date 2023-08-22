from pyspark.sql import SparkSession

KAFKA_INPUT_TOPIC_NAME_CONS = "input"
KAFKA_OUTPUT_TOPIC_NAME_CONS = "output"
KAFKA_BOOTSTRAP_SERVERS_CONS = "localhost:9092"

if __name__ == "__main__":
    print("PySpark Structured Streaming with Kafka Application Started …")

    spark = SparkSession \
        .builder \
        .appName("PySpark Structured Streaming with Kafka") \
        .master("local[*]") \
        .getOrCreate()

    inputDf = spark.readStream \
        .format("kafka") \
        .option("kafka.bootstrap.servers", KAFKA_BOOTSTRAP_SERVERS_CONS) \
        .option("subscribe", KAFKA_INPUT_TOPIC_NAME_CONS) \
        .load() \

    inputDf.printSchema()

    csvDF = inputDf.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")

    csvDF.writeStream\
        .outputMode("append")\
        .format("console")\
        .start()

    csvDF.printSchema()
