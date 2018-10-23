# Getting started with Apache Spark
	From inception to production
	by James A. Scott

<https://mapr.com/ebooks/spark/05-processing-tabular-data-with-spark-sql.html>

## Example 1
	// open interactive shell using scala
	$ spark-shell

	// create sequence of data 
	scala> val data = 1 to 50000
	// put data into a RDD
	scala> val sparkSample = sc.paralellize(data)
	// filter data 
	scala> sparkSample.filter(_ < 10).collect()


## Example 2 (processing tabular data with Spark SQL - scala interacive shell)
	
	// open interactive shell using scala
	$ spark-shell

	//  SQLContext entry point for working with structured data
	scala> val sqlContext = new org.apache.spark.sql.SQLContext(sc)
	// this is used to implicitly convert an RDD to a DataFrame.
	scala> import sqlContext.implicits._
	// Import Spark SQL data types and Row.
	scala> import org.apache.spark.sql._

	// load the data into a new RDD	
	scala> val ebayText = sc.textFile("ebay.csv")
	scala> ebayText.first()

	// define the schema using a case class
	scala> case class Auction(auctionid: String, bid: Float, bidtime: Float,bidder: String, bidderrate: Integer, openbid: Float, price: Float,item: String, daystolive: Integer)

	// create an RDD of Auction objects
	scala> val ebay = ebayText.map(_.split(",")).map(p => Auction(p(0), p(1).toFloat,p(2).toFloat,p(3),p(4).toInt,p(5).toFloat,
    p(6).toFloat,p(7),p(8).toInt))

	scala> ebay.first()

	scala> ebay.count()

	// change ebay RDD of Auction objects to a DataFrame
	scala> val auction = ebay.toDF()

	// Display the top 20 rows of DataFrame
	scala> auction.show()

	// How many auctions were held?
	scala> auction.select("auctionid").distinct.count

	// How many bids per item?
	scala> auction.groupBy("auctionid", "item").count.show

	// Get the auctions with closing price > 100
	scala> val highprice = auction.filter("price > 100")

	// display dataframe in a tabular format
	scala> highprice.show()

	// register the DataFrame as a temp table
	scala> auction.registerTempTable("auction")

	// How many bids per auction?
	scala> val results = sqlContext.sql("""SELECT auctionid, item,  count(bid) FROM auction
    GROUP BY auctionid, item""")

	// display dataframe in a tabular format
	scala> results.show()

	scala> val results = sqlContext.sql("""SELECT auctionid, MAX(price) FROM auction
    GROUP BY item,auctionid""")
	scala> results.show()

## Example 3 (Computing user profiles with Spark - python interactive shell)

	// open interactive shell using python
	$ pyspark

	from pyspark import SparkContext, SparkConf
	from pyspark.mllib.stat import Statistics
	import csv

	conf = SparkConf().setAppName('ListenerSummarizer')
	sc = SparkContext(conf=conf)


	trackfile = sc.textFile('tracks.csv')

	def make_tracks_kv(str):
    	l = str.split(",")
    	return [l[1], [[int(l[2]), l[3], int(l[4]), l[5]]]]

    # make a k,v RDD out of the input data
    tbycust = trackfile.map(lambda line: make_tracks_kv(line)).reduceByKey(lambda a, b: a + b)

    def compute_stats_byuser(tracks):
	    mcount = morn = aft = eve = night = 0
	    tracklist = []
	    for t in tracks:
	        trackid, dtime, mobile, zip = t
	        if trackid not in tracklist:
	            tracklist.append(trackid)
	        d, t = dtime.split(" ")
	        hourofday = int(t.split(":")[0])
	        mcount += mobile
	        if (hourofday < 5):
	            night += 1
	        elif (hourofday < 12):
	            morn += 1
	        elif (hourofday < 17):
	            aft += 1
	        elif (hourofday < 22):
	            eve += 1
	        else:
	            night += 1
	        return [len(tracklist), morn, aft, eve, night, mcount]

	# compute profile for each user
	custdata = tbycust.mapValues(lambda a: compute_stats_byuser(a))

	# compute aggregate stats for entire track history
	aggdata = Statistics.colStats(custdata.map(lambda x: x[1]))


	for k, v in custdata.collect():
    	unique, morn, aft, eve, night, mobile = v
    	tot = morn + aft + eve + night

    # persist the data, in this case write to a file
    with open('live_table.csv', 'wb') as csvfile:
        fwriter = csv.writer(csvfile, delimiter=' ',quotechar='|', quoting=csv.QUOTE_MINIMAL)
        fwriter.writerow(unique, morn, aft, eve, night, mobile)

    # do the same with the summary data
    with open('agg_table.csv', 'wb') as csvfile:
        fwriter = csv.writer(csvfile, delimiter=' ', quotechar='|', quoting=csv.QUOTE_MINIMAL)
        fwriter.writerow(aggdata.mean()[0], aggdata.mean()[1], aggdata.mean()[2], aggdata.mean()[3], aggdata.mean()[4], aggdata.mean()[5])