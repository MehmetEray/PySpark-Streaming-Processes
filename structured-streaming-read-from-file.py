from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder \
    .master("local[4]") \
    .appName("ReadFromFile") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()

lines = spark.readStream \
    .format("text") \
    .load("/Users/mehmeteraysurmeli/Desktop/streaming")

words = lines.select(f.explode(f.split(f.col("value"), " ")).alias("word"))
word_counts = words.groupBy("word").count().sort(f.desc("count"))
query = word_counts.writeStream \
    .outputMode("complete") \
    .format("console") \
    .start()
query.awaitTermination()
query.stop()

# ADD SOMETHING TO STREAMING FILE
