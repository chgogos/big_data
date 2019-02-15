from pyspark import SparkContext
from pyspark.streaming import StreamingContext
if __name__ == "__main__":
    sc = SparkContext(appName="PythonStreamingFiltration")
    ssc = StreamingContext(sc, 30)
    
    ds = ssc.socketTextStream('localhost', 8080)
    
    data = ds.map(lambda line: (line.split(" "))).map(lambda l: (l[0],int(l[1]),int(l[2]),int(l[3])))
    data_filter = data.filter(lambda line: line[2]==1)
    data_filter.pprint()
        
    ssc.start()
    ssc.awaitTermination()