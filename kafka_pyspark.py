import os

# os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.11:2.3.1 pyspark-shell'
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("WriteToKafka") \
    .getOrCreate()

df = spark.read.format("csv") \
    .option("header", True) \
    .load("/Users/mehmeteraysurmeli/Downloads/datasets_582088_1052144_Advertising.csv")

print(df.show(2))
df = df.selectExpr("_c0 as key", "TV as TV", "Radio as Radio", "Newspaper as Newspaper", "Sales as Sales")
print(df.show(2))

df_concated = df.select("key",
                        concat(
                            col('TV'), lit(","),
                            col('Radio'), lit(","),
                            col('Newspaper'), lit(","),
                            col('Sales')
                        ).alias("value")
                        )

print(df_concated.show(2))

df_concated \
  .write \
  .format("kafka") \
  .option("kafka.bootstrap.servers", "localhost:9092") \
  .option("topic", "deneme") \
  .save()