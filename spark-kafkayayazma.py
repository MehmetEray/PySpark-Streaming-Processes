from __future__ import print_function

from pyspark.sql import SparkSession
from pyspark.sql.functions import explode
from pyspark.sql.functions import split

spark = SparkSession\
 .builder\
 .appName("StructuredKafkaOrnegi")\
 .getOrCreate()

# lines = spark \
#     .readStream \
#     .format("socket") \
#     .option("host", "localhost") \
#     .option("port", 9092) \
#     .load()
#
# # Split the lines into words
# words = lines.select(
#    explode(
#        split(lines.value, " ")
#    ).alias("word")
# )
#
# # Generate running word count
# wordCounts = words.groupBy("word").count()
# query = wordCounts \
#     .writeStream \
#     .outputMode("complete") \
#     .format("console") \
#     .start()
#
# query.awaitTermination()


df = spark\
 .readStream\
 .format("kafka")\
 .option("kafka.bootstrap.servers", "localhost:9092")\
 .option("subscribe", "StatsTest")\
 .load()\
 .selectExpr("CAST(value AS STRING)")

query = df\
 .selectExpr("CAST(value AS STRING)")\
 .writeStream\
 .format('kafka')\
 .option('kafka.bootstrap.servers', 'localhost:9092')\
 .option('topic', 'StatsTestRes')\
 .option('checkpointLocation', '/Users/mehmeteraysurmeli/Desktop/streaming/')\
 .start()

query.awaitTermination()