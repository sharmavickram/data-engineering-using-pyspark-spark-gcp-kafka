from pyspark.sql import SparkSession
from pyspark.sql.functions import to_date, date_format, date_add, add_months, datediff

if __name__ == "__main__":
    spark = SparkSession.builder.appName("Spark SQL Date Functions").getOrCreate()

    df = spark.read.csv("D:\WORKBOOK\PYSPARK_BOOK\data-engineering-using-pyspark\data\csv\Airports2.csv", header=True, inferSchema=True)
    df.show(5)

    df.printSchema()

    #To_date(col) → Convert the string type containing date value to date format
    df2 = df.withColumn("Fly_date", to_date("Fly_date"))
    df2.printSchema()

    #Format "Fly_date" column with the "dd/MM/yyyy" format
    df.select("Fly_date", date_format("Fly_date", "dd/MM/yyyy").alias("Formatted_date")).show(3)

    #Date_add (start, days) → Add days to the date
    df.select("Fly_date", date_add("Fly_date", 2).alias("date_added")).show(5)

    ##Add_months(start, months)→Add Months to Date
    #Adding two months to the date columns and saving into a new dataframe
    test_df = df.select('Fly_date', add_months(df.Fly_date, 2).alias("months_added")).show(5)

    ##Datediff(end, start) → Returns the difference between two dates in days
    #Returns the difference between two dates in days
    #test_df.select("Fly_date", "months_added", datediff("months_added", "Fly_date").alias("date_diff")).show(3)


