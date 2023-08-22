from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    spark = SparkSession.builder.appName("create-new-col-from-breaking-col").getOrCreate()

    df = spark.createDataFrame([('vikram_1989',), ('anvi_2018',), ('anshu_1991',), ('sharma_2017',)], ['name'])

    newDF = df.withColumn("PersonName", f.split(df['name'], '\_')[0]). \
        withColumn("year", f.split(df['name'], '\_')[1])
    newDF.show()
