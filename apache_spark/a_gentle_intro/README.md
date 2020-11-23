# A gentle intro to Apache Spark

Παραδείγματα με Python

## The SparkSession

    >>> 

## Example 2

2015-summary.csv

    DEST_COUNTRY_NAME,ORIGIN_COUNTRY_NAME,count
    United States,Romania,15
    United States,Croatia,1
    United States,Ireland,344


    >>> flightData2015 = spark.read.option("inferSchema", "true").option("header", "true").csv("./data/2015-summary.csv")
    >>> flightData2015.take(3)
    [Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Romania', count=15), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Croatia', count=1), Row(DEST_COUNTRY_NAME='United States', ORIGIN_COUNTRY_NAME='Ireland', count=344)]

    >>> spark.conf.set("spark.sql.shuffle.partitions", 5)
    >>> flightData2015.sort("count").take(2)
    
    >>> flightData2015.sort("count").explain()
    == Physical Plan ==
    *(1) Sort [count#18 ASC NULLS FIRST], true, 0
    +- Exchange rangepartitioning(count#18 ASC NULLS FIRST, 5), true, [id=#50]
    +- FileScan csv [DEST_COUNTRY_NAME#16,ORIGIN_COUNTRY_NAME#17,count#18] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/E:/git_repos/big_data/apache_spark/a_gentle_intro/data/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string,ORIGIN_COUNTRY_NAME:string,count:int>

## Example 3 (DataFrames and SQL)

### A

    >>> flightData2015.createOrReplaceTempView("flight_data_2015")
    >>> sqlWay = spark.sql("""SELECT DEST_COUNTRY_NAME, count(*) AS NR FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME""")
    >>> sqlWay.take(2)
    [Row(DEST_COUNTRY_NAME='Moldova', NR=1), Row(DEST_COUNTRY_NAME='Bolivia', NR=1)]

    >>> dfWay = flightData2015.groupBy("DEST_COUNTRY_NAME").count()
    >>> dfWay.take(2)
    [Row(DEST_COUNTRY_NAME='Moldova', NR=1), Row(DEST_COUNTRY_NAME='Bolivia', NR=1)]

Τα explain plans είναι τα ίδια και για το Spark SQL και για το DataFrame

    >>> sqlWay.explain()
    == Physical Plan ==
    *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[count(1)])
    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#16, 5), true, [id=#265]
    +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[partial_count(1)])
        +- FileScan csv [DEST_COUNTRY_NAME#16] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/E:/git_repos/big_data/apache_spark/a_gentle_intro/data/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

    >>> dfWay.explain()
    == Physical Plan ==
    *(2) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[count(1)])
    +- Exchange hashpartitioning(DEST_COUNTRY_NAME#16, 5), true, [id=#265]
    +- *(1) HashAggregate(keys=[DEST_COUNTRY_NAME#16], functions=[partial_count(1)])
        +- FileScan csv [DEST_COUNTRY_NAME#16] Batched: false, DataFilters: [], Format: CSV, Location: InMemoryFileIndex[file:/E:/git_repos/big_data/apache_spark/a_gentle_intro/data/2015-summary.csv], PartitionFilters: [], PushedFilters: [], ReadSchema: struct<DEST_COUNTRY_NAME:string>

### B

    >>> spark.sql("select max(count) from flight_data_2015").take(1)
    [Row(max(count)=370002)]

    >>> from pyspark.sql.functions import max
    >>> flightData2015.select(max("count")).take(1)
    [Row(max(count)=370002)]

### C

    >>> maxSql = spark.sql("SELECT DEST_COUNTRY_NAME, sum(count) as destination_total FROM flight_data_2015 GROUP BY DEST_COUNTRY_NAME ORDER BY sum(count) DESC LIMIT 5")
    >>> maxSql.collect()
    [Row(DEST_COUNTRY_NAME='United States', destination_total=411352), Row(DEST_COUNTRY_NAME='Canada', destination_total=8399), Row(DEST_COUNTRY_NAME='Mexico', destination_total=7140), Row(DEST_COUNTRY_NAME='United Kingdom', destination_total=2025), Row(DEST_COUNTRY_NAME='Japan', destination_total=1548)]

    >>> from pyspark.sql.functions import desc
    >>> flightData2015.groupBy("DEST_COUNTRY_NAME").sum("count").withColumnRenamed("sum(count)", "destination_total").sort(desc("destination_total")).limit(5).collect()
    [Row(DEST_COUNTRY_NAME='United States', destination_total=411352), Row(DEST_COUNTRY_NAME='Canada', destination_total=8399), Row(DEST_COUNTRY_NAME='Mexico', destination_total=7140), Row(DEST_COUNTRY_NAME='United Kingdom', destination_total=2025), Row(DEST_COUNTRY_NAME='Japan', destination_total=1548)]


## A Tour of Spark's Toolset

### Production applications 

spark-submit

Από τον φάκελο που έχει εγκατασταθεί το Apache Spark (π.χ. C:\Spark)
    
    $ .\bin\spark-submit --master local .\examples\src\main\python\pi.py 10

### Datasets: Type-safe structured APIs

scala & java

    case class Flight(DEST_COUNTRY_NAME: String, ORIGIN_COUNTRY_NAME: String, count: BigInt
    
    val flightsDF = spark.read.parquet("/mnt/defg/flight-data/parquet/2010-summary.parquet/")
    val flights = flightsDF.as[Flight]

### Structured Streaming

    >>> staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data/retail-data/by-day/*.csv")
    >>> staticDataFrame.createOrReplaceTempView("retail_data")
    >>> staticSchema = staticDataFrame.schema

### Machine Learning and Advanced Analytics

MLlib: Παράδειγμα με K-Means (clustering algorithm)

    >>> staticDataFrame = spark.read.format("csv").option("header", "true").option("inferSchema", "true").load("data/retail-data/by-day/*.csv")

    >>> from pyspark.sql.functions import date_format, col
    >>> preppedDataFrame = staticDataFrame.na.fill(0).withColumn("day_of_week", date_format(col("InvoiceDate"), "EEEE")).coalesce(5)

    >>> trainDataFrame = preppedDataFrame.where("InvoiceDate < '2011-07-01'")   
    >>> testDataFrame = preppedDataFrame.where("InvoiceDate >= '2011-07-01'")
    >>> trainDataFrame.count()
    245903
    >>> testDataFrame.count()
    296006

    >>> from pyspark.ml.feature import StringIndexer
    >>> indexer = StringIndexer().setInputCol("day_of_week").setOutputCol("day_of_week_index")


    >>> from pyspark.ml.feature import OneHotEncoder
    >>> encoder = OneHotEncoder().setInputCol("day_of_week_index").setOutputCol("day_of_week_encoded")

    >>> from pyspark.ml.feature import VectorAssembler
    >>> vectorAssembler = VectorAssembler().setInputCols(["UnitPrice", "Quantity", "day_of_week_encoded"]).setOutputCol("features")


