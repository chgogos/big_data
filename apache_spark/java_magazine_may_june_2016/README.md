# Apache Spark example using spark-shell and processing weblogs

Java Magazine May-June 2016 - Apache Spark 101

Download weblogs from Java magazine download area: <https://goo.gl/ruJ629>

## Example 1

    # open weblogs, see some data
    $ cd weblogs
    $ ls -lh
    ...
    $ ls | wc -l
    108
    # examination of data (user is the 3rd item in each web log line)
    $ less 2013-09-15.log
    ...
    $ spark-shell
    # spark shell application UI <http://localhost:4040>
    scala> sc.version
    ...
    scala> val weblogs = sc.textFile("*")
    scala> weblogs.count()
    ...
    scala> weblogs.first()
    ...
    scala> weblogs.take(2)
    ...
    # Action takeSample, false=noreplacement (each row can be selected at most one time) 
    scala> weblogs.takeSample(false,3)
    ...

## Example 2

    # Number of distinct users, user is the 3rd item in each web log line (transformation map->transformation distinct)
    scala> val userIds = weblogs.map(item => item.split(" ")(2)).distinct()
    scala> userIds.count()
    ...

## Example 3

    # Group IPs that each user has been connected from (transformation map->transformation map)
    scala> var userIPpairs = weblogs.map(item => item.split(" ")).map(s => (s(2),s(0)))
    scala> userIPpairs.first()
    ...
    scala> val userIPs = userIPpairs.groupByKey()
    scala> userIPs.first()
    ...
    scala> userIPs.takeSample(false,5)

## Example 4

    # Top 50 users according to the number of IPs they connected from (transformation map->transformation sortByKey->transformation map)
    scala> val top = userIPs.map(v=>(v._2.toSeq.length, v._1)).sortByKey(false).map(item=>item.swap)
    scala> top.take(50)
    ...
    scala> :quit