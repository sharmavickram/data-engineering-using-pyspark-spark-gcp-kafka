from pyspark.sql import SparkSession
from pyspark.sql.functions import regexp_replace, when

if __name__ == "__main__":
    spark = SparkSession.builder.appName("rest api to dataframe").getOrCreate()

    address = [(1, "14851 Jeffrey Rd", "DE"),
               (2, "43421 Margarita St", "NY"),
               (3, "13111 Siemon Ave", "CA"),
               (4, "98851 Jeffrey Rd", "DE")]
    df = spark.createDataFrame(address, ["id", "address", "state"])
    df.show()

    #Replace String Column Values
    df.withColumn("address",regexp_replace("address",'Rd','Road')).show()

    #Replace Column Values Conditionally
    df.withColumn("address",
                  when(df["address"].endswith("Rd"), regexp_replace("address",'Rd','Road'))\
                  .when(df["address"].endswith("Ave"), regexp_replace("address","Ave","Avenue"))\
                  .when(df["state"].startswith("NY"), regexp_replace("state","NY","New York"))).show()