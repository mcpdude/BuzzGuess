from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext, SparkSession, DataFrameWriter

import sys

import re as re

user = sys.argv[1]
password = sys.argv[2]

path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"
regex = "(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
url = 'jdbc:postgresql://10.0.0.25:5432/postgres:5432'
mode = 'append'
props = {"user": user, "password": password}


if __name__ == '__main__':


	conf = SparkConf()
	conf.setAppName('CopyChat')
	conf.set('spark.master', 'spark://localhost:7077')

	spark = SparkSession.builder.appName('CopyChat').getOrCreate()

	file = spark.read.json(path)
	comments = file.filter(file.type=='comment')
	small_comments = comments.select('by', 'text', 'time')
	# sentences = small_comments.select('by', 'text', 'time', func.split('text', regex).alias('sentences'))
	# sentences_exploded = sentences\
	#     .select('by', 'time', func.explode(func.split('text', regex)).alias('sentence')).where(func.length('sentence') > 5)
	# sentences_exploded.printSchema()

	small_comments.registerTempTable("stuff")

	small_comments.printSchema()

	small_comments.show()

	table = 'stuff'

	my_writer = DataFrameWriter(small_comments)
	my_writer.jdbc(url, table, mode, props)
