from __future__ import print_function

import sys
import random

from datetime import datetime
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"

master = 'spark://ip-10-0-0-15.us-west-2.compute.internal:7077'

regex = "(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"

if __name__ == "__main__":

    dt = datetime.now()

    spark = SparkSession\
        .builder\
        .appName("test" + str(dt.second))\
        .getOrCreate()

    

    def sentence_split(comment):
        sentences = comment.compile("(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s").split(comment)

        return len(sentences)

    file = spark.read.json(path)

    comments = file.filter(file.type=='comment')

    small_comments = comments.select('by', 'text', 'time')

    small_comments.show()

    sentences = small_comments.select('text', func.split('value', regex).alias('sentence'))

    sentences.printSchema()

    sentences.show()








# if __name__ == "__main__":

#     spark = SparkSession\
#         .builder\
#         .appName("test")\
#         .getOrCreate()

#     conf = SparkConf().setAppName(appName).setMaster(master)
#     sc = SparkContext(conf=conf)

#     print(path, "\n\n\n\n\n\n\n\n\n\n")
#     file = spark.read.json(path)

#     comments = file.filter(file.type=='comment')

#     small_comments = comments.select('by', 'text', 'time')

#     small_comments.show()

#     small_comments.write.csv('mycsv.csv')

#     small_comments.write.csv('mycsv.csv').save("hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/mycsv.csv")

#     spark.stop()