from pyspark import SparkConf, SparkContext
from pyspark.sql import SQLContext

import sys

import re as re

user = sys.argv[1]
password = sys.argv[2]

path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"
regex = "(?<!\w\.\w.)(?<![A-Z][a-z]\.)(?<=\.|\?)\s"
url = 'jdbc:postgresql://tester.ccarw5e1afmj.us-west-2.rds.amazonaws.com:5432/postgres'
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

	table = 'stuff'

	my_writer = DataFrameWriter(comments)
	my_writer.jdbc(url, table, mode, props)
