
path  = "hdfs://ip-10-0-0-15.us-west-2.compute.internal:9000/user/HNI_2018-05.json"

file = spark.read.json(path)

comments = file.filter(file.type=='comment')

small_comments = comments.select('by', 'text', 'time')

return len(small_comments.count())