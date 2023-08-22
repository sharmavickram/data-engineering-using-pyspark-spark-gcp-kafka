from pyspark.sql import SparkSession
from pyspark.sql.functions import when, expr, col

if __name__ == "__main__":
    spark = SparkSession.builder.appName("when-otherwise on dataframe").getOrCreate()

    data = [('Vikram', '', 'Sharma', '1989-01-01', 'M', 3000000),
            ('Anshu', 'Kumari', '', '1991-04-20', 'F', 400000),
            ('Anvi', '', 'Sharma', '2018-11-09', 'F', 400000)]
    schema = ["fname", "mname", "lname", "DoB", "Gender", "salary"]

    df = spark.createDataFrame(data,schema)
    df.show()

    #syntax: when(condition).otherwise(default).
    # when() function take 2 parameters, first param takes a condition and second takes a literal value or Column,
    # if condition evaluates to true then it returns a value from second param.

    df2 = df.withColumn("new_gender", when(df["gender"] == "M", "Male")
                  .when(df["gender"] == "F", "Female")
                  .when(df["gender"].isNull(), "")
                  .otherwise(df["gender"]))
    df2.printSchema()
    df2.show()


