from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *
import os
os.environ['PYSPARK_SUBMIT_ARGS'] = './bin/spark-submit --packages org.apache.spark:spark-sql-kafka-0-10_2.13:3.0.1 ...'
spark = SparkSession.builder \
    .master("local[4]") \
    .appName("ReadFromKafka") \
    .getOrCreate()

df = spark \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "deneme") \
    .load()
df2 = df.selectExpr(" CAST(key AS STRING)", "CAST(value AS STRING)")
words = df2.select(explode(split(col("value"), " ")).alias("value"))
word_counts = words.groupBy("value").count().sort(desc("count"))
query = word_counts.writeStream \
    .format("console") \
    .outputMode("complete") \
    .trigger(processingTime='1 seconds') \
    .start()

