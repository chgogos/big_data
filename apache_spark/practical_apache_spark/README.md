# Practical Apache Spark

<https://www.kdnuggets.com/2019/01/practical-apache-spark-10-minutes.html>

## Part 1

    # Start spark using Scala
    $ spark-shell
    ...
    $ :quit
    $ Start spark using Python
    $ pyspark
    $ textfile = sc.textFile('README.md')
    $ textfile.take(3)
    [u'# Practical Apache Spark', u'', u'<https://www.kdnuggets.com/2019/01/practical-apache-spark-10-minutes.html>']
    $ counts = textfile.flatMap(lambda line: line.split(" ")).map(lambda word: (word,1)).reduceByKey(lambda a, b: a + b)
    $ counts.take(5)
    [(u'', 1688), (u'y)', 2), (u'"pet_wid"],', 1), (u'[26]', 2), (u'0.0|[0.67184222484015...|', 1)]

## Part 2 (RDD)

    $ pyspark
    ...
    SparkSession available as 'spark'.
    # Transformations
    $ num = sc.parallelize([4, 6, 6, 1, 3, 0, 2, 2, 2])
    $ result = num.map(lambda x: x**2)
    $ result.take(10)
    [16, 36, 36, 1, 9, 0, 4, 4, 4]
    $ result = num.filter(lambda x: x >= 3)
    $ result.take(10)
    [4, 6, 6, 3]
    $ result = num.distinct()
    $ result.take(10)
    $ num2 = sc.parallelize([5, 5, 8, 2, 2, 1, 7, 3, 3])
    $ result = num.union(num2)
    $ result.take(20)
    [4, 6, 6, 1, 3, 0, 2, 2, 2, 5, 5, 8, 2, 2, 1, 7, 3, 3]
    $ result = num.intersection(num2)
    $ result.take(20)
    [2, 1, 3]
    $ result = num.subtract(num2)
    $ result.take(20)
    [0, 4, 6, 6]
    # Actions
    $ num.count()
    9
    $ num.countByValue()
    defaultdict(<type 'int'>, {0: 1, 1: 1, 2: 3, 3: 1, 4: 1, 6: 2})
    $ num.collect()
    [4, 6, 6, 1, 3, 0, 2, 2, 2]
    $ num.takeOrdered(5)
    [0, 1, 2, 2, 2]
    $ num.reduce(lambda x, y: x + y)
    [26]
    $ num.fold(0, lambda x,y : x + y)
    [26]
    +++

## Part 3 (DataFrames and SQL)

    # Create DataFrame from CSV file
    $ data = spark.read.format("csv").option("delimiter", ";").option("header", True).load("movielens.csv")
    # Query using DataFrame API
    $ data.show(3)
    +-------+--------------------+--------------------+------+------+----------+
    |movieId|               title|              genres|userId|rating| timestamp|
    +-------+--------------------+--------------------+------+------+----------+
    |      1|    Toy Story (1995)|Adventure|Animati...|     7|   3.0| 851866703|
    |      2|      Jumanji (1995)|Adventure|Childre...|    15|   2.0|1134521380|
    |      3|Grumpier Old Men ...|      Comedy|Romance|     5|   4.0|1163374957|
    +-------+--------------------+--------------------+------+------+----------+
    $ data.select("title", "rating").show(3)
    +--------------------+------+
    |               title|rating|
    +--------------------+------+
    |    Toy Story (1995)|   3.0|
    |      Jumanji (1995)|   2.0|
    |Grumpier Old Men ...|   4.0|
    +--------------------+------+
    $ data.filter(data['rating'] < 3.0).show(3)
    +-------+--------------------+--------------------+------+------+----------+
    |movieId|               title|              genres|userId|rating| timestamp|
    +-------+--------------------+--------------------+------+------+----------+
    |      2|      Jumanji (1995)|Adventure|Childre...|    15|   2.0|1134521380|
    |     11|American Presiden...|Comedy|Drama|Romance|    15|   2.5|1093028381|
    |     14|        Nixon (1995)|               Drama|    15|   2.5|1166586286|
    +-------+--------------------+--------------------+------+------+----------+
    $ data.groupBy(data['rating']).count().orderBy('rating').show()
    +------+-----+
    |rating|count|
    +------+-----+
    |   0.5|  227|
    |   1.0|  574|
    |   1.5|  298|
    |   2.0|  831|
    |   2.5|  611|
    |   3.0| 1790|
    |   3.5| 1141|
    |   4.0| 2156|
    |   4.5|  554|
    |   5.0|  884|
    +------+-----+
    # Basic statistics
    $ data.describe('rating').show()
    +-------+------------------+
    |summary|            rating|
    +-------+------------------+
    |  count|              9066|
    |   mean| 3.223527465254798|
    | stddev|1.1572344494786881|
    |    min|               0.5|
    |    max|               5.0|
    +-------+------------------+
    # Using SQL to query data
    $ data.createOrReplaceTempView("movielens")
    $ spark.sql("select * from movielens where rating < 3").show(3)
    +-------+--------------------+--------------------+------+------+----------+
    |movieId|               title|              genres|userId|rating| timestamp|
    +-------+--------------------+--------------------+------+------+----------+
    |      2|      Jumanji (1995)|Adventure|Childre...|    15|   2.0|1134521380|
    |     11|American Presiden...|Comedy|Drama|Romance|    15|   2.5|1093028381|
    |     14|        Nixon (1995)|               Drama|    15|   2.5|1166586286|
    +-------+--------------------+--------------------+------+------+----------+
    # Create DataFrame from RDD
    # Load data from text file to a RDD
    $ rdd = sc.textFile("movielens.txt")\
    .map(lambda line: line.split(";"))\
    .map(lambda splits: (int(splits[0]), splits[1], splits[2]))
    $ rdd.take(3)
    [(1, u'Toy Story (1995)', u'Adventure|Animation|Children|Comedy|Fantasy'), (2, u'Jumanji (1995)', u'Adventure|Children|Fantasy'), (3, u'Grumpier Old Men (1995)', u'Comedy|Romance')]
    $ from pyspark.sql.types import *
    $ id_field = StructField("id", IntegerType(), True)
    $ title_field = StructField("title", StringType(), True)
    $ genres_field = StructField("genres", StringType(), True)
    $ schema = StructType([id_field, title_field, genres_field])
    $ movielens = spark.createDataFrame(rdd, schema)
    $ movielens.show(3)
    +---+--------------------+--------------------+
    | id|               title|              genres|
    +---+--------------------+--------------------+
    |  1|    Toy Story (1995)|Adventure|Animati...|
    |  2|      Jumanji (1995)|Adventure|Childre...|
    |  3|Grumpier Old Men ...|      Comedy|Romance|
    +---+--------------------+--------------------+
    # Create RDD from DataFrame
    $ movielensRDD = movielens.rdd
    $ movielensRDD.take(3)
    [Row(id=1, title=u'Toy Story (1995)', genres=u'Adventure|Animation|Children|Comedy|Fantasy'), Row(id=2, title=u'Jumanji (1995)', genres=u'Adventure|Children|Fantasy'), Row(id=3, title=u'Grumpier Old Men (1995)', genres=u'Comedy|Romance')]
    # Load data from JSON files
    $ movies = spark.read.json('movielens.json')
    $ movies.createOrReplaceTempView("movies")
    $ nice_movies = spark.sql("select * from movies where rating > 4.9")
    $ nice_movies.show(3)
    +-------------+-------+------+---------+--------------------+------+
    |       genres|movieId|rating|timestamp|               title|userId|
    +-------------+-------+------+---------+--------------------+------+
    |Drama|Romance|     17|   5.0|835355681|Sense and Sensibi...|     2|
    |Drama|Romance|     28|   5.0|854714394|   Persuasion (1995)|    67|
    |  Crime|Drama|     30|   5.0|848161285|Shanghai Triad (Y...|    86|
    +-------------+-------+------+---------+--------------------+------+
    # Write to JSON file
    $ nice_movies.write.json('nice_movies')
    # Write to Apache Parquet files
    $ data.write.parquet('movielens.parquet')
    $ parquet = spark.read.parquet('movielens.parquet')
    $ parquet.createOrReplaceTempView('parquetlens')
    $ just_movies = spark.sql("select title, rating from parquetlens where rating between 2 and 5")
    $ just_movies.show(3)
    +--------------------+------+
    |               title|rating|
    +--------------------+------+
    |    Toy Story (1995)|   3.0|
    |      Jumanji (1995)|   2.0|
    |Grumpier Old Men ...|   4.0|
    +--------------------+------+

## Part 4 (MLib)

    $ df = spark.read.csv("bezdekIris.data", inferSchema=True).toDF("sep_len", "sep_wid", "pet_len", "pet_wid", "label")
    df.show(5)
    +-------+-------+-------+-------+-----------+
    |sep_len|sep_wid|pet_len|pet_wid|      label|
    +-------+-------+-------+-------+-----------+
    |    5.1|    3.5|    1.4|    0.2|Iris-setosa|
    |    4.9|    3.0|    1.4|    0.2|Iris-setosa|
    |    4.7|    3.2|    1.3|    0.2|Iris-setosa|
    |    4.6|    3.1|    1.5|    0.2|Iris-setosa|
    |    5.0|    3.6|    1.4|    0.2|Iris-setosa|
    +-------+-------+-------+-------+-----------+
    $ from pyspark.ml.linalg import Vectors
    $ from pyspark.ml.feature import VectorAssembler
    $ vector_assembler = VectorAssembler(inputCols=["sep_len", "sep_wid", "pet_len", "pet_wid"], outputCol="features")
    $ df_temp = vector_assembler.transform(df)
    $ df = df_temp.drop('sep_len', 'sep_wid', 'pet_len', 'pet_wid')
    $ df.show(3)
    +-----------+-----------------+
    |      label|         features|
    +-----------+-----------------+
    |Iris-setosa|[5.1,3.5,1.4,0.2]|
    |Iris-setosa|[4.9,3.0,1.4,0.2]|
    |Iris-setosa|[4.7,3.2,1.3,0.2]|
    +-----------+-----------------+
    $ from pyspark.ml.feature import StringIndexer
    $ l_indexer = StringIndexer(inputCol="label", outputCol="labelIndex")
    $ df = l_indexer.fit(df).transform(df)
    $ df.show(3)
    # import libraries for classification
    $ from pyspark.ml.classification import DecisionTreeClassifier
    $ from pyspark.ml.evaluation import MulticlassClassificationEvaluator
    # random split training and test data
    $ (trainingData, testData) = df.randomSplit([0.7, 0.3])
    $ dt = DecisionTreeClassifier(labelCol="labelIndex", featuresCol="features")
    $ model = dt.fit(trainingData)
    # Test model on testData
    $ predictions = model.transform(testData)
    # Show some results on testData
    $ predictions.select("prediction", "labelIndex").show(5)
    +----------+----------+
    |prediction|labelIndex|
    +----------+----------+
    |       0.0|       0.0|
    |       0.0|       0.0|
    |       0.0|       0.0|
    |       0.0|       0.0|
    |       0.0|       0.0|
    +----------+----------+
    # Estimate the accuracy of the model
    $ evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="accuracy")
    $ accuracy = evaluator.evaluate(predictions)
    $ print("Test Error = %g " % (1.0 - accuracy))
    Test Error = 0.0930233
    # Print small summary of the model
    $ print(model)
    DecisionTreeClassificationModel (uid=DecisionTreeClassifier_a5a521d73758) of depth 5 with 11 node
    # Random Forest Classifier
    $ from pyspark.ml.classification import RandomForestClassifier
    $ rf = RandomForestClassifier(labelCol="labelIndex",featuresCol="features", numTrees=10)
    $ model = rf.fit(trainingData)
    $ predictions = model.transform(testData)
    $ evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="accuracy")
    $ accuracy = evaluator.evaluate(predictions)
    $ print("Test Error = %g " % (1.0 - accuracy))
    Test Error = 0.0697674
    $ print(model)
    RandomForestClassificationModel (uid=RandomForestClassifier_8ef2eae4e13c) with 10 trees
    # Naive Bayes classifier
    $ splits = df.randomSplit([0.6, 0.4], 1234)
    $ train = splits[0]
    $ test = splits[1]
    $ from pyspark.ml.classification import NaiveBayes
    $ nb = NaiveBayes(labelCol="labelIndex", featuresCol="features", smoothing=1.0, modelType="multinomial")
    $ model = nb.fit(train)
    $ predictions = model.transform(test)
    $ predictions.select("label", "labelIndex", "probability", "prediction").show(5)
    +-----------+----------+--------------------+----------+
    |      label|labelIndex|         probability|prediction|
    +-----------+----------+--------------------+----------+
    |Iris-setosa|       0.0|[0.72723788653438...|       0.0|
    |Iris-setosa|       0.0|[0.64170595827692...|       0.0|
    |Iris-setosa|       0.0|[0.67184222484015...|       0.0|
    |Iris-setosa|       0.0|[0.68647236934182...|       0.0|
    |Iris-setosa|       0.0|[0.79151826954673...|       0.0|
    +-----------+----------+--------------------+----------+
    $ evaluator = MulticlassClassificationEvaluator(labelCol="labelIndex", predictionCol="prediction", metricName="accuracy")
    $ accuracy = evaluator.evaluate(predictions)
    $ print("Test Error = %g " % (1.0 - accuracy))
    Test Error = 0.176471
    $ print(model)
    NaiveBayes_d67b1d7045aa

## Part 5 (Streaming)

    # Start stream server (terminal A)
    $ nc -lk 8080
    # Start streaming service (terminal B)
    $ spark-submit streaming1.py
    # Input text at terminal A
    01/02/2001 1 1 23
    01/02/2001 2 0 12
    01/02/2001 3 1 22
    02/02/2001 1 1 25
    02/02/2001 2 0 15
    02/02/2001 3 0 10
    # results at terminal B (spark)
    # number of measurements for each sensor
    -------------------------------------------
    Time: 2019-02-15 15:20:30
    -------------------------------------------
    (1, 2)
    (2, 2)
    (3, 2)

## Part 6 (GraphX)

    # start Apache Spark for Scala
    $ spark-shell
    $ import org.apache.spark.graphx.Edge
    $ import org.apache.spark.graphx.Graph
    $ import org.apache.spark.graphx.lib._
    # sample verices array (city, population)
    $ val verArray = Array(
        (1L, ("Philadelphia", 1580863)),
        (2L, ("Baltimore", 620961)),
        (3L, ("Harrisburg", 49528)),
        (4L, ("Wilmington", 70851)),
        (5L, ("New York", 8175133)),
        (6L, ("Scranton", 76089)))
    verArray: Array[(Long, (String, Int))] = Array((1,(Philadelphia,1580863)), (2,(Baltimore,620961)), (3,(Harrisburg,49528)), (4,(Wilmington,70851)), (5,(New York,8175133)), (6,(Scranton,76089)))
    $ val edgeArray = Array(
        Edge(2L, 3L, 113),
        Edge(2L, 4L, 106),
        Edge(3L, 4L, 128),
        Edge(3L, 5L, 248),
        Edge(3L, 6L, 162),
        Edge(4L, 1L, 39),
        Edge(1L, 6L, 168),
        Edge(1L, 5L, 130),
        Edge(5L, 6L, 159))
    edgeArray: Array[org.apache.spark.graphx.Edge[Int]] = Array(Edge(2,3,113), Edge(2,4,106), Edge(3,4,128), Edge(3,5,248), Edge(3,6,162), Edge(4,1,39), Edge(1,6,168), Edge(1,5,130), Edge(5,6,159))
    $ val verRDD = sc.parallelize(verArray)
    $ val edgeRDD = sc.parallelize(edgeArray)
    # graph creation
    $ val graph = Graph(verRDD, edgeRDD)
    # Filtration by vertices (example: cities with population more than 50000)
    $ graph.vertices.filter {
        case (id, (city, population)) => population > 50000
    }.collect.foreach {
        case (id, (city, population)) => println(s"The population of $city is $population")
    }
    The population of Wilmington is 70851
    The population of Philadelphia is 1580863
    The population of New York is 8175133
    The population of Scranton is 76089
    The population of Baltimore is 620961
    # Triplets RDD (vertex, edge, vertex)
    $ for (triplet <- graph.triplets.collect) {
        println(s"""The distance between ${triplet.srcAttr._1} and  ${triplet.dstAttr._1} is ${triplet.attr} kilometers""")
    }
    The distance between Baltimore and  Harrisburg is 113 kilometers
    The distance between Baltimore and  Wilmington is 106 kilometers
    The distance between Harrisburg and  Wilmington is 128 kilometers
    The distance between Harrisburg and  New York is 248 kilometers
    The distance between Harrisburg and  Scranton is 162 kilometers
    The distance between Wilmington and  Philadelphia is 39 kilometers
    The distance between Philadelphia and  New York is 130 kilometers
    The distance between Philadelphia and  Scranton is 168 kilometers
    The distance between New York and  Scranton is 159 kilometers
    # Filtration by edges (example: pairs of cities with direct distance lower than 150km )
    $ graph.edges.filter {
        case Edge(city1, city2, distance) => distance < 150
    }.collect.foreach {
        case Edge(city1, city2, distance) =>
        println(s"The distance between $city1 and $city2 is $distance")
    }
    # Aggregation (example: find total poulation of neighboring cities)
    # Add reverse direction edge for each edge of the graph
    $ val undirectedEdgeRDD = graph.reverse.edges.union(graph.edges)
    $ val graph = Graph(verRDD, undirectedEdgeRDD)
    $ val neighbors = graph.aggregateMessages[Int](ectx => ectx.sendToSrc(ectx.dstAttr._2), _ + _)
    $ neighbors.foreach(println(_))
    (3,8943034)
    (4,2251352)
    (1,8322073)
    (5,1706480)
    (6,9805524)
    (2,120379)
