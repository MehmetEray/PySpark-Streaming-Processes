from pyspark.sql import SparkSession

ss = SparkSession.builder \
    .master("local[4]") \
    .appName('RDDWordCount') \
    .getOrCreate()
sc = ss.sparkContext
data = "/Users/mehmeteraysurmeli/Downloads/simple_data.txt"
data_rdd = sc.textFile(data)
data_rdd_without_header = data_rdd.filter(lambda x: 'sirano' not in x)

print(data_rdd.take(20))
print(data_rdd_without_header.take(20))

def salary(line):
    job = line.split(',')[3]
    salary = float(line.split(',')[5])
    return (job,salary)

job_salary_pair_RDD = data_rdd_without_header.map(salary)
print(job_salary_pair_RDD.take(20))

job_salary2 = job_salary_pair_RDD.mapValues(lambda x: (x,1))
print(job_salary2.take(20))

job_salary_RBK = job_salary2.reduceByKey(lambda x,y: (x[0] + y[0] , y[0] + y[1]))
print(job_salary_RBK.take(20))

job_salary_mapvalues = job_salary_RBK.mapValues(lambda x: x[0] / x[1])
print(job_salary_mapvalues.take(10))