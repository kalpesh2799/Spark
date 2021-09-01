from pyspark import SparkContext, SparkConf, SQLContext
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when, split, explode, window, count
from pyspark.sql.types import StructType, StructField, StringType, IntegerType


master = 'local'
appName = 'PySpark_Dataframe DB Operations'

config = SparkConf().setAppName(appName).setMaster(master)
sc = SparkContext(conf=config)
# You will need to create the sqlContext
sqlContext = SQLContext(sc)


jdbcURL = 'jdbc:postgresql://localhost:5432/postgres'
user = 'postgres'
password = 'kalraj'

dfFromDb = sqlContext.read.format('jdbc') \
    .option('url', jdbcURL) \
    .option('user', user) \
    .option('password', password) \
    .option('dbtable', 'countries') \
    .load()
dfFromDb.printSchema()
dfFromDb.show()

