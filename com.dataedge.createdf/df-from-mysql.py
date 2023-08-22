from pyspark.sql import SparkSession

spark = SparkSession.builder.appName("DF creation").master("local[*]").getOrCreate()


dataframe_mysql = spark.read.format("jdbc")\
        .options(
            url="jdbc:mysql://localhost:3307/dataengdb",
            driver="com.mysql.jdbc.Driver",
            dbtable="training",
            user="root",
            password="dataedge").load()

dataframe_mysql.show()


