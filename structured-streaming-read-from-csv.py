from pyspark.sql import SparkSession
from pyspark.sql.types import *
import pyspark.sql.functions as f

spark = SparkSession.builder \
.master("local[4]") \
.appName("ReafDromCsv") \
.config("spark.driver.memory","2g") \
.config("spark.executor.memory","4g") \
.getOrCreate()
# schema tanımla
schema_simple_data = StructType(
[
    StructField("sirano", IntegerType(), True),
    StructField("isim", StringType(), True),
    StructField("yas", IntegerType(), True),
    StructField("meslek", StringType(), True),
    StructField("sehir", StringType(), True),
    StructField("aylik_gelir", DoubleType(), True)
]
)
df = spark.readStream \
.format("csv") \
.option("header",True) \
.option("sep", ",") \
.schema(schema_simple_data) \
.load("/Users/mehmeteraysurmeli/Desktop/streaming")

meslek_grp_ort_gelir = df.groupBy("meslek") \
.agg(f.avg("aylik_gelir").alias("ort_gelir")) \
.sort(f.desc("ort_gelir"))


query = meslek_grp_ort_gelir.writeStream \
.outputMode("complete") \
.format("console") \
.start()
query.awaitTermination()

query.stop()
