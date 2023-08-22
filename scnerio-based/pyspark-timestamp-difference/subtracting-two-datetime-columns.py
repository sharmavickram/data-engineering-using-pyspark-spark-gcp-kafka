from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StringType, StructField, IntegerType

from pyspark.sql.functions import udf

if __name__ == "__main__":
    spark = SparkSession.builder.appName("subtracting-two-datetime-columns").getOrCreate()

    # Build sample data
    rdd = spark.sparkContext.parallelize([('101', '2014-02-13T12:36:14.899', '2014-02-13T12:31:56.876'),
                                          ('102', '2014-02-13T12:35:37.405', '2014-02-13T12:32:13.321'),
                                          ('103', '2014-02-13T12:36:03.825', '2014-02-13T12:32:15.229'),
                                          ('1O4', '2014-02-13T12:37:05.460', '2014-02-13T12:32:36.881'),
                                          ('1O5', '2014-02-13T12:36:52.721', '2014-02-13T12:33:30.323')])

    schema = StructType([StructField('ID', StringType(), True),
                         StructField('EndDateTime', StringType(), True),
                         StructField('StartDateTime', StringType(), True)])
    df = spark.createDataFrame(rdd, schema)

    df.printSchema()
    df.show(truncate=False)


    # define timedelta function (obtain duration in seconds)
    def time_delta(y, x):
        from datetime import datetime
        end = datetime.strptime(y, '%Y-%m-%dT%H:%M:%S.%f')
        start = datetime.strptime(x, '%Y-%m-%dT%H:%M:%S.%f')
        delta = (end - start).total_seconds()
        return delta


    # register as a UDF
    f = udf(time_delta, IntegerType())

    # Apply function
    df2 = df.withColumn('Duration', f(df.EndDateTime, df.StartDateTime))
    df2.printSchema()
    df2.show(truncate=False)