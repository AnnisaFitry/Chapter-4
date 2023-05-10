csv_df = spark.read.options(header='true',inferSchema='true').csv("/home/annisa/Downloads/people.csv")

csv_df.printSchema()

csv_df.select('name', 'job').write.options(codec="org.apache.hadoop.io.compress.GzipCodec").csv('newcars.csv')