from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder.appName("dataframe to mysql-table") \
        .master("local[*]")\
        .config("spark.jars", "mysql-connector-java-8.0.13.jar")\
        .getOrCreate()

    data = [('Vikram', '', 'Sharma', '1989-01-01', 'M', 3000000),
            ('Anshu', 'Kumari', '', '1991-04-20', 'F', 400000),
            ('Anvi', '', 'Sharma', '2018-11-09', 'F', 400000)]
    schema = ["fname", "mname", "lname", "DoB", "Gender", "salary"]

    df = spark.createDataFrame(data, schema)
    df.show()

    df.write \
      .format("jdbc") \
      .mode("overwrite")\
      .option("driver","com.mysql.cj.jdbc.Driver") \
      .option("url", "jdbc:mysql://localhost:3307/sqlshack") \
      .option("dbtable", "emp1") \
      .option("user", "root") \
      .option("password", "admin") \
      .save()

    dataframe_mysql = spark.read.format("jdbc")\
        .options(
            url="jdbc:mysql://localhost:3307/sqlshack",
            driver="com.mysql.jdbc.Driver",
            dbtable="company",
            user="root",
            password="password").load()

    dataframe_mysql.show()