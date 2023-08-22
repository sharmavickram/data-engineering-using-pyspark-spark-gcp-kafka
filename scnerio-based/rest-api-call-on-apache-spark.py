import requests
from pyspark.sql import SparkSession

url = "https://raw-tutorial.s3.eu-west-1.amazonaws.com/patients.json"

if __name__ == "__main__":
    spark = SparkSession.builder.appName("rest api to dataframe").getOrCreate()
    response = requests.get(url)
    rdd = spark.sparkContext.parallelize([response.text])
    df = spark.read.json(rdd)

    df.printSchema()
    df.show(truncate=False)
    df2 = df.selectExpr('inline(diagnosis)')
    df2.show(truncate=False)

    df2.select("diag_id","patient_id").show()

