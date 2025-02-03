# Apache Spark Quick Start

* <https://spark.apache.org/docs/latest/quick-start.html>
* <https://spark.apache.org/docs/latest/rdd-programming-guide.html>
* <https://spark.apache.org/docs/latest/sql-programming-guide.html>


## Example using Python
### Start pyspark
```
$ pyspark
Python 3.10.12 | packaged by conda-forge | (main, Jun 23 2023, 22:41:52) [Clang 15.0.7 ] on darwin
Type "help", "copyright", "credits" or "license" for more information.
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/02/03 12:53:38 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /__ / .__/\_,_/_/ /_/\_\   version 3.5.4
      /_/

Using Python version 3.10.12 (main, Jun 23 2023 22:41:52)
Spark context Web UI available at http://192.168.1.17:4040
Spark context available as 'sc' (master = local[*], app id = local-1738580018380).
SparkSession available as 'spark'.
>>>
```


### Basic example
```
>>> textFile = spark.read.text("1984.txt")
>>> textFile.count()
10323
>>> textFile.first()
Row(value='Project Gutenberg Australia')
>>> linesWithWinston = textFile.filter(textFile.value.contains("Winston"))
>>> linesWithWinston.count()
520
```

### More on dataset operations

Find the largest number of words in a line
```
>>> from pyspark.sql import functions as sf
>>> textFile.select(sf.size(sf.split(textFile.value, "\s+")).name("numWords")).agg(sf.max(sf.col("numWords"))).collect()
[Row(max(numWords)=19)]
```

Word count
```
>>> wordCounts = textFile.select(sf.explode(sf.split(textFile.value, "\s+")).alias("word")).groupBy("word").count()
>>> wordCounts.collect()[10]
[Row(word='online', count=1), Row(word='working,', count=6), Row(word='those', count=46), Row(word='still', count=135), Row(word='transmitted', count=1), Row(word='some', count=178), Row(word='By', count=18), Row(word='Apart', count=2), Row(word='flashed', count=4), Row(word='sabotage,', count=2)]
```

### Caching
```
>>> linesWithWinston.cache()
DataFrame[value: string]

>>> linesWithWinston.count()
520

>>> linesWithWinston.count()
520
```

### Self-Contained Applications
Count lines with a, count lines with b
```
$ /opt/homebrew/bin/spark-submit SimpleApp.py
25/02/03 13:34:10 INFO SparkContext: Running Spark version 3.5.4
25/02/03 13:34:10 INFO SparkContext: OS info Mac OS X, 15.2, aarch64
25/02/03 13:34:10 INFO SparkContext: Java version 17.0.14
...
Lines with a: 8452, lines with b: 4722
...
25/02/03 13:34:13 INFO ShutdownHookManager: Deleting directory /private/var/folders/bf/b6jc2d693t16_7ckk7n7x9dh0000gn/T/spark-79180744-b685-4d61-a283-d6eee0378e82
```

---


## Example using Scala

### Start spark-shell
```
$ spark-shell
Setting default log level to "WARN".
To adjust logging level use sc.setLogLevel(newLevel). For SparkR, use setLogLevel(newLevel).
25/02/03 12:16:15 WARN NativeCodeLoader: Unable to load native-hadoop library for your platform... using builtin-java classes where applicable
Spark context Web UI available at http://192.168.1.17:4040
Spark context available as 'sc' (master = local[*], app id = local-1738577775462).
Spark session available as 'spark'.
Welcome to
      ____              __
     / __/__  ___ _____/ /__
    _\ \/ _ \/ _ `/ __/  '_/
   /___/ .__/\_,_/_/ /_/\_\   version 3.5.4
      /_/

Using Scala version 2.12.18 (OpenJDK 64-Bit Server VM, Java 17.0.14)
Type in expressions to have them evaluated.
Type :help for more information.
```

### Basic example
```
scala> val textFile = spark.read.textFile("1984.txt")
textFile: org.apache.spark.sql.Dataset[String] = [value: string]

scala> textFile.count()
res0: Long = 10325

scala> textFile.first()
res1: String = Project Gutenberg Australia

scala> val linesWithWinston = textFile.filter(line => line.contains("Winston"))
linesWithWinston: org.apache.spark.sql.Dataset[String] = [value: string]

scala> textFile.filter(line => line.contains("Winston")).count()
res3: Long = 520
```

### More on dataset operations

Find the largest number of words in a line
```
scala> textFile.map(line => line.split(" ").size).reduce((a, b) => if (a > b) a else b)
res4: Int = 19
```

Word count
```
scala> val wordCounts = textFile.flatMap(line => line.split(" ")).groupByKey(identity).count().collect()
wordCounts: Array[(String, Long)] = Array((online,1), (working,,6), (those,46), (still,135), (transmitted,1), (some,178), (By,18), (Apart,2), (flashed,4), (sabotage,,2), (emotions.,1), (few,98), (persist,1), (eye.,5), (hope,3), (cubs,1), (connected,6), (waters,1), (pools,5), ('And,19), (superseded,3), (disgrace,,1), (involving,6), (quacked,2), (painters,1), (outfit,2), (wife.,2), (embrace,2), (saucepans,2), (imitation,1), (arguments,6), (vague,,2), ('AS,1), (only----',1), (people'd,1), (lady's,1), (say--he,1), (hitched,1), (filing,1), (cautious,1), (everyday,7), (inner,7), (curtain.,2), (recognize,5), (standards,7), (febrile,1), (frightened'.,1), (doubts,1), (squealing,1), (Antarctic,,1), (nineteen-forties,,1), (art,1), (inimical,3), (accumulation,2), (currents...
```

### Caching
```
scala> linesWithWinston.cache()
res5: linesWithWinston.type = [value: string]

scala> linesWithWinston.count()
res6: Long = 520

scala> linesWithWinston.count()
res7: Long = 520
```