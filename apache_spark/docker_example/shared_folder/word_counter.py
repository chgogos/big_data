from pyspark.sql import SparkSession
import re

# execution mode,App Name
spark=SparkSession.builder.master('spark://localhost:7077').appName('WordCounter').getOrCreate()
sc=spark.sparkContext

wordfile=sc.textFile("enlarged_book.txt")

counts=wordfile.flatMap(lambda line:re.split(r'\W+',line)).map(lambda word:(word,1)).reduceByKey(lambda x,y:x+y)

final_output=counts.collect()
for word,count in final_output:
    print(f'{word},{count}')