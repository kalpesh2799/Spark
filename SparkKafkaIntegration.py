import os
from pyspark import SparkConf, SparkContext, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split

os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.1.2 ' \
                                    '/home/abhay/MyHome/CodeHome/PythonWS/PySpark_Examples/SparkKafkaIntegration.py'

master = 'local'
appName = 'Spark Kafka Integration'

config = SparkConf().setAppName(appName).setMaster(master)

sc = SparkContext(conf=config)
# You will need to create the sqlContext
sqlContext = SQLContext(sc)
ss = SparkSession(sc)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')


print('=================================')
print('Read traindata over a kafka and parse it into a DF')
print('=================================')


df = ss \
    .readStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "localhost:9092") \
    .option("subscribe", "eDBDA_BDM2") \
    .load()

df.printSchema()

trainData = df.selectExpr('CAST(value AS STRING)')

trainDF = trainData \
    .withColumn('trainNo', split(trainData.value, '\\|').getItem(0)) \
    .withColumn('TrainIn', split(trainData.value, '\\|').getItem(1)) \
    .withColumn('TrainOut', split(trainData.value, '\\|').getItem(2)) \
    .withColumn('Speed', split(trainData.value, '\\|').getItem(3)) \
    .withColumn('DirIn', split(trainData.value, '\\|').getItem(4)) \
    .withColumn('DirOut', split(trainData.value, '\\|').getItem(5)) \
    .withColumn('Duration', split(trainData.value, '\\|').getItem(6)) \
    .drop('value')

trainDF.printSchema()

# Below 1 statement will not work
# trainDF.show()

query = trainDF.writeStream.outputMode('append').format('console').trigger(processingTime='10 seconds').start()
query.awaitTermination()
