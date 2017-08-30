 cluster-spark-wordcount.py
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import HiveContext
from pyspark.sql import Row
from pyspark.sql.types import StructType, StructField, IntegerType, StringType, DateType
from pyspark.sql import Window
from pyspark.sql.functions import mean

HDFS_MASTER = 'quickstart.cloudera'

conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('spark-wordcount')
conf.set('spark.executor.instances', 10)
sc = SparkContext(conf=conf)
#sc2 = new SQLContext(sc);

schema = StructType([ \
           StructField("date", StringType(), True), \
           StructField("time", StringType(), True), \
           StructField("open", StringType(), True), \
           StructField("high", StringType(), True), \
           StructField("low", StringType(), True), \
           StructField("close", StringType(), True), \
           StructField("volume", StringType(), True), \
           StructField("ticker", StringType(), True) ])

sqlContext = SQLContext(sc)
hsc = HiveContext(sc)
df = hsc.read.format("com.databricks.spark.csv").option("delimeter",",").option("header","false").option("inferSchema","true").load("hdfs://quickstart.cloudera:8020/home/cloudera/sqoop-import/daily/part-m-*", schema=schema)
df.printSchema()
df.show(5)
#df.select("ticker, date, close").show(5)

#df.registerTempTable("tempTable")
#sqlContext.sql("SELECT emp_no,gender from tempTable").show()
#sqlContext.sql("SELECT count(*) as totalRows from tempTable").show()
#sqlContext.sql("SELECT count(*) as maleRows from tempTable where gender = 'M'").show()
#sqlContext.sql("SELECT count(*) as femaleRows from tempTable where gender = 'F'").show()

window = Window.partitionBy('ticker').orderBy('date').rowsBetween(-1, 1)
#window
moving_avg = mean(df['close']).over(window)
moving_avg
df2 = df.withColumn('moving_avg', moving_avg)
df2.show()

print "Done"
