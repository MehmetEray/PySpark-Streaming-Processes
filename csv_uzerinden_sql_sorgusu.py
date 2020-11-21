from pyspark.sql import SparkSession
spark = SparkSession.builder \
.master("local[4]") \
.appName("Csv-Üzeri-SQL") \
.config("spark.executor.memory","4g") \
.config("spark.driver.memory","2g") \
.getOrCreate()


retailDF = spark.read \
.option("header","True") \
.option("inferSchema","True") \
.option("sep",";") \
.csv("/Users/mehmeteraysurmeli/Downloads/OnlineRetail.csv")

print(retailDF.limit(5).toPandas().head()
      )
print(retailDF.cache())

print(retailDF.createOrReplaceTempView("tablo"))

print(spark.sql("""

SELECT Country, SUM(UnitPrice) UnitPrice 
FROM tablo
GROUP BY Country
ORDER BY UnitPrice DESC




""").show(20))