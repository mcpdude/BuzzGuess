from __future__ import print_function

import sys
import random
from operator import add
from pyspark import SparkContext, SparkConf
from pyspark.sql import SparkSession

path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"


if __name__ == "__main__":

    conf = SparkConf().setAppName('check it out').setMaster(master)
    sc = SparkContext(conf=conf)

    def sentence_split(comment):
        sentences = comment.compile("(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s").split(comment)

        return len(sentences)


    file = spark.read.json(path)

    comments = file.filter(file.type=='comment')

    small_comments = comments.select('by', 'text', 'time')

    rdd = small_comments.rdd

    distData = sc.parallelize(rdd)

    distData.save("hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/mycsv.csv")












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