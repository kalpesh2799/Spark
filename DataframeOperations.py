from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql.functions import col, when

master = 'local'
appName = 'PySpark_Dataframe Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext
sqlContext = SQLContext(sc)

if sc:
    print(sc.appName)
else:
    print('Could not initialise pyspark session')

print('====================== Dataframe Operations ================= ')
print()
print('=================================')
print('Load Data into dataframe')
print('=================================')
data1 = [(1, 1), (2, 2), (3, 3), (4, 4), (5, 5), (6, 6)]
parallelData1 = sc.parallelize(data1)
print('Datasets created')

print('=================================')
print('Create a DF without schema')
print('=================================')
df = parallelData1.toDF()
df.show()
print('=================================')

print()
print('=================================')
print('Create a DF with schema directly as an argument')
print('=================================')
df = parallelData1.toDF(['col1', 'col2'])
df.show()
print('=================================')

print()
print('=================================')
print('Create a DF with schema with array argument')
print('=================================')
colNameArray = ['col1', 'col2']
df = parallelData1.toDF(colNameArray)
df.show()
print('=================================')


print()
print('=================================')
print('Add a new column')
print('=================================')
# Remember to import "from pyspark.sql.functions import col"
df = df.withColumn('newCol', col('col1'))
df.show()
print('=================================')


print()
print('=================================')
print('Rename a new column')
print('=================================')
# Remember to import "from pyspark.sql.functions import col"
# and from pyspark.sql import functions as F
# df = df.withColumnRenamed(F.col("col2"), F.col("newColUpdated"))
df = df.withColumnRenamed('col2', 'newColUpdated')
df.show()
print('=================================')


print()
print('=================================')
print('Drop a new column')
print('=================================')
# Remember to import "from pyspark.sql.functions import col"
df = df.drop('newColUpdated')
df.show()
print('=================================')


print()
print('=================================')
print('Select column')
print('=================================')
df = df.select('col1')
df.show()
print('=================================')

print()
print('=================================')
print('Select column')
print('=================================')
df = df.selectExpr('col1 as someNewColumn')
df.show()
print('=================================')


print()
print('=================================')
print('Read from a file without header')
print('=================================')
df = sqlContext.read.option('header', False).option('delimiter', '|').csv('file:///home/abhay/MyHome/DataHome/TrainDataset/Train_Dataset.csv')
df.show()
df.select('_c0').show()
print('=================================')


print()
print('=================================')
print('Read from a file with header')
print('=================================')
df = sqlContext.read.option('header', True).option('delimiter', '|').csv('file:///home/abhay/MyHome/DataHome/TrainDataset/Train_Dataset_withHeader.csv')
df.show()
df.printSchema()
df.select('TrainNo').show()
print('=================================')

print()
print('=================================')
print('Filter data using filter')
print('=================================')
df.filter('TrainNo == 1001').show()
print('=================================')

print()
print('=================================')
print('Filter data using where')
print('=================================')
df.where('TrainNo == 1001').show()
print('=================================')


print()
print('=================================')
print('Using with and when')
print('=================================')
# Remember to import 'from pyspark.sql.functions import when'
df.withColumn('TrainNo', when(col('TrainNo') == 1001, 'OneThousandOne')
              .when(col('TrainNo') == 1010, 'OneThousandTen')
              .otherwise('No Train')).show()
print('=================================')
