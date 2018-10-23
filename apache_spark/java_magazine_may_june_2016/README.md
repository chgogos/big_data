# Apache Spark example using spark-shell and processing weblogs

Java Magazine May-June 2016 - Apache Spark 101

Install apache spark locally

Download weblogs from Java magazine download area: https://goo.gl/ruJ629
unzip to ~

## Example 0
	
	// open weblogs, see some data 

	$ cd ~
	$ cd weblogs
	$ ls -lh 
	$ ls | wc -l  // number of files (=108)
	$ less 2013-09-15.log  // user is the 3rd item in each web log line
	$ cd ~ 

	$ spark-shell // start spark 
	http://localhost:4040  // spark shell application UI

	scala> sc.version 
	scala> val weblogs = sc.textFile("weblogs/*")
	scala> weblogs.count()   // action count
	scala> weblogs.first()   // action first
	scala> weblogs.take(2)   // action take
	
	// action takeSample, false=noreplacement (each row can be selected at most one time) 
	scala> weblogs.takeSample(false,3)   

## Example 1

	// Number of distinct users, user is the 3rd item in each web log line

	// transformation map-> transformation distinct
	scala> val userIds = weblogs.map(item => item.split(" ")(2)).distinct() 
	scala> userIds.count() // action count

## Example 2

	// Group IPs that each user connected from

	// 2 transformations map->map
	scala> var userIPpairs = weblogs.map(item => item.split(" ")).map(s => (s(2),s(0))) 
	scala> userIPpairs.first() // action first
	scala> val userIPs = userIPpairs.groupByKey() // transformation groupByKey
	scala> userIPs.first() // action first
	scala> userIPs.takeSample(false,5) // action takeSample

## Example 3

	// Top 50 users according to the number of IPs they connected from
	
	// 3 transformations map->sortByKey->map
	scala> val top = userIPs.map(v=>(v._2.toSeq.length, v._1)).sortByKey(false).map(item=>item.swap)
	scala> top.take(50) // action take
	scala> :quit