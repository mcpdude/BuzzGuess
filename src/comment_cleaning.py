from __future__ import print_function

import sys
from operator import add
from pyspark.sql import SparkSession

path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"


if __name__ == "__main__":

    spark = SparkSession\
        .builder\
        .appName("test")\
        .getOrCreate()


    print(path)
	file = spark.read.json(path)

	comments = file.filter(file.type=='comment')

	small_comments = comments.select('by', 'text', 'time')

	print(en(small_comments.count()))

	spark.stop()