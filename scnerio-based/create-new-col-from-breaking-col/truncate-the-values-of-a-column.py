from pyspark.sql import SparkSession
import pyspark.sql.functions as f

if __name__ == "__main__":
    spark = SparkSession.builder.appName("truncate-the-values-of-a-column").getOrCreate()

    df = spark.createDataFrame(
        [('0035','vikram',), ('0004','anvi',), ('0032','anshu',)], ['age','name'])
    df.show()

    #df.withColumn('age', f.substring(f.col('age'), 2, 3)).show()
    df.withColumn("age", f.expr("substring(age, 3, length(age))")).show()

    df.write.format('xml')

