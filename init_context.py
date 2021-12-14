import os

from pyspark.sql import SparkSession
from pyspark import SparkConf,SparkContext
import findspark

def init_context():
    findspark.init("C:\Program Files\Spark")
    os.environ['PYSPARK_PYTHON'] = r'C:\Users\room102sys2\AppData\Local\Programs\Python\Python39'
    app_name = input("Give the app name:")
    sp = SparkSession.builder.appName(app_name).getOrCreate()
    sc = sp.sparkContext
    return sc, sp