import pandas as pd
from pyspark import SparkContext, SparkConf
from pyspark.shell import spark
import findspark
from pyspark.sql import SparkSession

ss = SparkSession.builder \
    .master("local[4]") \
    .appName('RDDWordCount') \
    .getOrCreate()
sc = ss.sparkContext
data = "/Users/mehmeteraysurmeli/Downloads/OnlineRetail.csv"
data_rdd = sc.textFile(data)
print(data_rdd.count())
print(data_rdd.take(10))

words = data_rdd.flatMap(lambda line : line.split(" "))
print(words.take(10))

word_counts = words.map(lambda word : (word,1))
print(word_counts.take(10))

word_counts_Reduce_by_key = word_counts.reduceByKey(lambda x,y: x + y)
print(word_counts_Reduce_by_key.take(10))

word_counts_Reduce_by_key_crossed = word_counts_Reduce_by_key.map(lambda x: (x[1], x[0]))
print(word_counts_Reduce_by_key_crossed.take(10))

print(word_counts_Reduce_by_key_crossed.sortByKey(False).take(30))

