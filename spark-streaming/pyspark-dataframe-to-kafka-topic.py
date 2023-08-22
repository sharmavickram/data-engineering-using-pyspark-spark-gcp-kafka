import os

from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("pyspark-dataframe-to-kafka-topic").getOrCreate()

    topic = "dataedge"
    kafka_servers = "localhost:29092"
    spark_version = '3.1.2'
    os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.13:{}'.format(spark_version)

    # producer = KafkaProducer(bootstrap_servers=['localhost:9092'],
    # value_serializer= lambda x: x.encode('utf-8'))

    df = spark.read.csv("D:/WORKBOOK/PYSPARK_BOOK/data-engineering-using-pyspark/data/csv/students.csv",
                        inferSchema=True, header=True)
    df.show(10)




df.printSchema()



