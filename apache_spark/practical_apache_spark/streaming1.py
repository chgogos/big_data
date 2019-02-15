from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingCountByValue")
    ssc = StreamingContext(sc, 30)
    
    ds = ssc.socketTextStream('localhost', 8080)
            
    data = ds.map(lambda line: int(line.split(" ")[1]))
    data_count = data.countByValue()
    data_count.pprint()
        
    ssc.start()
    ssc.awaitTermination()