from pyspark.sql import SparkSession
from pyspark.conf import SparkConf
from pyspark.sql.functions import explode, split, col
from pyspark.sql.functions import desc

# Aşağıdaki ayarları bilgisayarınızın belleğine göre değiştirebilirsiniz
spark = SparkSession.builder \
.master("local[4]") \
.appName("Dataset-Olusturmak") \
.config("spark.executor.memory","4g") \
.config("spark.driver.memory","2g") \
.getOrCreate()

# sparkContext'i kısaltmada tut
sc = spark.sparkContext
hikaye_df = spark.read.text("/Users/mehmeteraysurmeli/Downloads/omer_seyfettin_forsa_hikaye.txt")
print(hikaye_df.show(3,truncate=False))
kelimeler = hikaye_df.select(explode(split(col("value"), " ")).alias("value"))
print(kelimeler.show(3))
print(kelimeler.groupBy("value").count().orderBy(desc("count")).show(10))