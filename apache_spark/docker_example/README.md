# PageRank Example in Spark

* [6.824 Distributed Systems](https://pdos.csail.mit.edu/6.824/)
  * http://nil.csail.mit.edu/6.824/2020/schedule.html
  * [Notes](http://nil.csail.mit.edu/6.824/2020/notes/l-spark.txt)
  * [Lecture 15: Big data : Spark](https://www.youtube.com/watch?v=mzIoSW-cInA&list=PLrw6a1wE39_tb2fErI4-WkMbsvGQk9_UB&index=17)
  * [Spark Faq](http://nil.csail.mit.edu/6.824/2020/papers/spark-faq.txt)
* https://github.com/abbas-taher/pagerank-example-spark2.0-deep-dive

**Run scala spark program SparkPageRank over urldata.txt for 10 iterations**
```
/opt/spark/bin/run-example SparkPageRank urldata.txt 10
```

**Run scala spark commands line by line**
```
val lines = spark.read.textFile("urldata.txt").rdd
val links1 = lines.map{ s => val parts = s.split("\\s+");(parts(0), parts(1))}
val links2 = links1.distinct()
val links3 = links2.groupByKey()
val links4 = links3.cache()
var ranks = links4.mapValues(v => 1.0)

// 1st iteration
val jj = links4.join(ranks)
val contribs = jj.values.flatMap{case (urls, rank) =>urls.map(url => (url, rank / urls.size))}
ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

// 2nd iteration
val jj = links4.join(ranks)
val contribs = jj.values.flatMap{case (urls, rank) =>urls.map(url => (url, rank / urls.size))}
ranks = contribs.reduceByKey(_ + _).mapValues(0.15 + 0.85 * _)

val output = ranks.collect()
output.foreach(tup => println(s"${tup._1} has rank:  ${tup._2} ."))
```