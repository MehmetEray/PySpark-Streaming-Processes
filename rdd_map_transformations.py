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
print(data_rdd_withoutheader.take(20))
def canceled_price(line):
    iscanceled = True if (line.split(";")[0].startswith("C")) else False
    quantity = float(line.split(";")[3])
    price = float(line.split(";")[5].replace(",","."))
    total = quantity * price
    return (iscanceled,total)

canceled_total = data_rdd_withoutheader.map(canceled_price)
print(canceled_total.take(10))