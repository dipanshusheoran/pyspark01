import sys,os
from pyspark.sql import *
os.environ['PYSPARK_PYTHON'] = sys.executable
os.environ['PYSPARK_DRIVER_PYTHON'] = sys.executable
spark = SparkSession \
        .builder \
        .appName("HelloSpark") \
        .master("local[2]") \
        .getOrCreate()


columns = ["language","users_count"]
data = [("Java", "20000"), ("Python", "100000"), ("Scala", "3000").("NodeJS","1234")]

df = spark.createDataFrame(data,columns)

df.show()