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


firstline = data_rdd.first()
# print(firstline)
firstline_rdd = sc.parallelize([firstline])
data_rdd_withoutheader = data_rdd.subtract(firstline_rdd)


print(data_rdd_withoutheader.filter(lambda x: int(x.split(';')[3]) > 50).take(10))
print(data_rdd_withoutheader.filter(lambda x: 'COFFEE' in (x.split(';')[2])).take(10))


def double_filter(x):
    quantity = int(x.split(';')[3])
    description = x.split(';')[2]
    return (quantity > 30) & ('LIGHTS' in description)


print(data_rdd_withoutheader.filter(lambda x: double_filter(x)).take(10))
