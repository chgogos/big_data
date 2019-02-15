from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingNetworkMaxTemperature")
    ssc = StreamingContext(sc, 30)
    ds = ssc.socketTextStream('localhost', 8080)  
    temperature = ds.map(lambda line: int(line.split(" ")[3]))
    result = temperature.reduce(max)
    result.pprint()   
    ssc.start()
    ssc.awaitTermination()