# cluster-spark-wordcount.py
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType

HDFS_MASTER = 'quickstart.cloudera'

conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('spark-wordcount')
conf.set('spark.executor.instances', 10)
sc = SparkContext(conf=conf)
#sc2 = new SQLContext(sc); 

schema = StructType([ \
           StructField("emp_no", IntegerType(), True), \
           StructField("birthdate", DateType(), True), \
           StructField("fname", StringType(), True), \
           StructField("lname", StringType(), True), \
           StructField("gender", StringType(), True), \
           StructField("hiredate", DateType(), True) ])

sqlContext = SQLContext(sc)
df = sqlContext.read.format("com.databricks.spark.csv").option("delimeter",",").option("header","false").option("inferSchema","true").load("hdfs://quickstart.cloudera:8020/home/cloudera/sqoop-import/employees/part-m-*", schema=schema)
df.printSchema()
df.show(5)
df.select("fname").show(5)

df.registerTempTable("tempTable")
#sqlContext.sql("SELECT emp_no,gender from tempTable").show()
sqlContext.sql("SELECT count(*) as totalRows from tempTable").show()
sqlContext.sql("SELECT count(*) as maleRows from tempTable where gender = 'M'").show()
sqlContext.sql("SELECT count(*) as femaleRows from tempTable where gender = 'F'").show()
           
print "Done"
