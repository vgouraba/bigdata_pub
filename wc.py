# cluster-spark-wordcount.py
from pyspark import SparkConf
from pyspark import SparkContext
from pyspark import SQLContext
from pyspark.sql import Row

HDFS_MASTER = 'quickstart.cloudera'

conf = SparkConf()
conf.setMaster('yarn-client')
conf.setAppName('spark-wordcount')
conf.set('spark.executor.instances', 10)
sc = SparkContext(conf=conf)
#sc2 = new SQLContext(sc); 

distFile = sc.textFile('hdfs://{0}:8020/home/cloudera/sqoop-import/employees/part-m-*'.format(HDFS_MASTER))

nonempty_lines = distFile.filter(lambda x: len(x) > 0)
print 'Nonempty lines', nonempty_lines.count()

words = nonempty_lines.flatMap(lambda x: x.split(' '))

wordcounts = words.map(lambda x: (x, 1)) \
                  .reduceByKey(lambda x, y: x+y) \
                  .map(lambda x: (x[1], x[0])).sortByKey(False)

print 'Top 100 words:'
#print wordcounts.take(100)

#print 'Print as DF:'
#val linesDF = sc2.textFile('hdfs://{0}:8020/home/cloudera/sqoop-import/part-m-00000').toDF("line")
#val wordsDF = linesDF.explode("line","word")((line: String) => line.split(" "))
#val wordCountDF = wordsDF.gropuBy("word").count()
#wordCountDF.show()

sqlContext = SQLContext(sc);
rdd = sc.parallelize(wordcounts.take(100))
#rdd = wordcounts.take(100) 
#people = rdd.map(lambda x: Row(id=x[0], rowid=x[1], dob=x[2], nfirst=x[3], nlast=x[4], sex=x[5], djoin=x[6])) 
people = rdd.map(lambda x: Row(id=x[0], details=x[1]))
schemaPeople = sqlContext.createDataFrame(people);
print "Type"
type(schemaPeople)
print "PrintSchema"
schemaPeople.printSchema()
print "Head 5"
schemaPeople.head(5)
print "Done"
