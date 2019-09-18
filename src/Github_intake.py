from __future__ import print_function

import sys
import random

from datetime import datetime
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession
import pyspark.sql.functions as func

old_path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"

path = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/2015-01-01-15.json"

write_path = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/"

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

    # file.printSchema()

    comments = file.filter(file.type=='IssuesCommentEvent')

    comments.show()


    # comments = file.filter(file.type=='comment')

    # small_comments = comments.select('by', 'text', 'time')

    # small_comments.show()






    # sentences = small_comments.select('by', 'text', 'time', func.split('text', regex).alias('sentences'))

    # sentences.printSchema()

    # sentences.show()

    # sentences_exploded = sentences \
    #     .select('by', 'time', func.explode(func.split('text', regex)) \
    #     .alias('sentence'))

    # sentences_exploded.printSchema()

    # sentences_exploded.show()

    # sentences_exploded.coalesce(1).write.csv(write_path + 'hey', mode = 'overwrite', header = 'true')

    








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