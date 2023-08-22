from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark-Batch-Produce-To-Kafka-demo").getOrCreate()

    spark.sparkContext.setLogLevel("ERROR")

    data = [('Vikram', '', 'Sharma', '1989-01-01', 'M', 3000000),
            ('Anshu', 'Kumari', '', '1991-04-20', 'F', 400000),
            ('Anvi', '', 'Sharma', '2018-11-09', 'F', 400000)]
    schema = ["fname", "mname", "lname", "DoB", "Gender", "salary"]

    df = spark.createDataFrame(data, schema)

    df.write\
        .format("kafka")\
        .option("kafka.bootstrap.servers", "localhost:29092")\
        .option("topic", "batch")\
        .save()
