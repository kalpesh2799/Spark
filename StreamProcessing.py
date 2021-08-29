from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType

master = 'local'
appName = 'PySpark_Streaming Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext for SparkSQL
sqlContext = SQLContext(sc)
# You will need to create the SparkSession for streaming
ss = SparkSession(sc)

if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

# print()
# print('=================================')
# print('Read traindata over a socket')
# print('=================================')
# trainData = ss.readStream.format('socket') \
#     .option('host', 'localhost') \
#     .option('port', 9876) \
#     .load()
#
# trainData.printSchema()
#
# query = trainData.writeStream \
#     .outputMode('append').format('console').start()
#
# query.awaitTermination()


print('=================================')
print('Read traindata over a socket and parse it into a DF')
print('=================================')
ss = SparkSession(sc)
if ss:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')
# Run this command before starting your program ==> $ nc -lk 9876
trainData = ss.readStream.format('socket') \
    .option('host', 'localhost') \
    .option('port', 9876) \
    .load()
trainData.printSchema()

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

query = trainDF.writeStream \
    .outputMode('append').format('console').start()
query.awaitTermination()
print('=================================')


print('=================================')
print('Read train data over a file location')
print('=================================')

trainSchema = StructType([StructField('TrainNo', StringType(), True),
                          StructField('TrainIn', IntegerType(), True),
                          StructField('TrainOut', IntegerType(), True),
                          StructField('Speed', IntegerType(), True),
                          StructField('DirIn', StringType(), True),
                          StructField('DirOut', StringType(), True),
                          StructField('Duration', StringType(), True)])

# Please modify the param to CSV with some existing directory
trainData = ss.readStream.schema(trainSchema) \
    .option('header', False) \
    .option('delimiter', '|') \
    .csv('/tmp/streamingLocation')

# D://tmp//streamingLocation

trainData.printSchema()

query = trainData.writeStream \
    .outputMode('append').format('console').start()

query.awaitTermination()

print('=================================')

# Checkpointing example to be taken with Kafka after kafka installation

# Untested code below:
# print('=================================')
# print('Aggregate train data over a windowed timeframe')
# print('=================================')
# trainData = ss.readStream.format('socket') \
#     .option('host', 'localhost') \
#     .option('port', 9876) \
#     .load()
#
# trainData.printSchema()
#
# trainDF = trainData \
#     .withColumn('trainNo', split(trainData.value, '\\|').getItem(0)) \
#     .withColumn('TrainIn', split(trainData.value, '\\|').getItem(1)) \
#     .withColumn('TrainOut', split(trainData.value, '\\|').getItem(2)) \
#     .withColumn('Speed', split(trainData.value, '\\|').getItem(3)) \
#     .withColumn('DirIn', split(trainData.value, '\\|').getItem(4)) \
#     .withColumn('DirOut', split(trainData.value, '\\|').getItem(5)) \
#     .withColumn('Duration', split(trainData.value, '\\|').getItem(6)) \
#     .drop('value')
#
# trainDF = trainDF\
#     .groupby(window(trainDF.trainNo, '10 seconds'))\
#     .agg(count(trainDF.trainNo).alias('TotalTrainsInLast10Seconds'))
#
# query = trainDF.writeStream \
#     .outputMode('append').format('console').start()
#
# query.awaitTermination()
