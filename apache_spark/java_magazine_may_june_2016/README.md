# Apache Spark example using spark-shell and processing weblogs

[Java Magazine May-June 2016 - Apache Spark 101](http://www.javamagazine.mozaicreader.com/MayJune2016/LinkedIn/29/0#&pageSet=14&page=0)

Download weblogs from Java magazine download area: <https://goo.gl/ruJ629>

## Example 1

```{sh}
# open weblogs, see some data
$ cd weblogs
$ ls -lh
total 170200
-rw-r--r--@ 1 chgogos  staff   509K Feb 26  2016 2013-09-15.log
-rw-r--r--@ 1 chgogos  staff   473K May 20  2015 2013-09-16.log
...
$ ls | wc -l
108
# examination of data (user is the 3rd item in each web log line)
$ less 2013-09-15.log
...
$ spark-shell
# spark shell application UI <http://localhost:4040>
scala> sc.version
res: String = 2.4.0
scala> val weblogs = sc.textFile("*")
scala> weblogs.count()
res: Long = 574023
scala> weblogs.first()
res6: String = 146.11.102.141 - 40486 [16/Oct/2013:23:59:01 +0100] "GET /KBDOC-00261.html HTTP/1.0" 200 19161 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L"
scala> weblogs.take(2)
res: Array[String] = Array(146.11.102.141 - 40486 [16/Oct/2013:23:59:01 +0100] "GET /KBDOC-00261.html HTTP/1.0" 200 19161 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L", 146.11.102.141 - 40486 [16/Oct/2013:23:59:01 +0100] "GET /theme.css HTTP/1.0" 200 14967 "http://www.loudacre.com"  "Loudacre Mobile Browser Sorrento F11L")
# Action takeSample, false=noreplacement (each row can be selected at most one time) 
scala> weblogs.takeSample(false,3)
res: Array[String] = Array(1.226.231.214 - 45650 [22/Nov/2013:13:55:58 +0100] "GET /theme.css HTTP/1.0" 200 16274 "http://www.loudacre.com"  "Loudacre Mobile Browser Titanic 1100", 4.137.25.62 - 91878 [25/Dec/2013:04:02:24 +0100] "GET /theme.css HTTP/1.0" 200 6439 "http://www.loudacre.com"  "Loudacre Mobile Browser Ronin S3", 152.132.132.11 - 80 [18/Oct/2013:22:53:46 +0100] "GET /KBDOC-00261.html HTTP/1.0" 200 8797 "http://www.loudacre.com"  "Loudacre CSR Browser")
```

## Example 2

```{sh}
# Number of distinct users, user is the 3rd item in each web log line (transformation map->transformation distinct)
scala> val userIds = weblogs.map(item => item.split(" ")(2)).distinct()
scala> userIds.count()
res: Long = 12582
```

## Example 3

```{sh}
# Group IPs that each user has been connected from (transformation map->transformation map)
scala> var userIPpairs = weblogs.map(item => item.split(" ")).map(s => (s(2),s(0)))
scala> userIPpairs.first()
res: (String, String) = (40486,146.11.102.141)
scala> val userIPs = userIPpairs.groupByKey()
scala> userIPs.first()
res: (String, Iterable[String]) = (99066,CompactBuffer(140.106.99.216, 140.106.99.216, 52.3.99.222, 52.3.99.222, 52.3.99.222, 52.3.99.222, 254.21.251.202, 254.21.251.202, 120.55.224.170, 120.55.224.170, 77.122.77.207, 77.122.77.207, 20.231.169.91, 20.231.169.91, 161.173.92.141, 161.173.92.141, 73.94.122.230, 73.94.122.230, 107.232.109.180, 107.232.109.180, 195.67.16.237, 195.67.16.237))
scala> userIPs.takeSample(false,5)
res: Array[(String, Iterable[String])] = Array((24592,CompactBuffer(173.219.197.67, 173.219.197.67, 173.219.197.67, 173.219.197.67, 238.152.231.113, 238.152.231.113)), (33438,CompactBuffer(200.21.1.135, 200.21.1.135, 236.7.81.212, 236.7.81.212, 229.22.122.20, 229.22.122.20)), (10285,CompactBuffer(34.47.173.91, 34.47.173.91)), (86312,CompactBuffer(80.182.138.216, 80.182.138.216, 17.11.195.90, 17.11.195.90, 17.11.195.90, 17.11.195.90, 17.35.174.197, 17.35.174.197, 43.231.112.134, 43.231.112.134, 43.231.112.134, 43.231.112.134, 80.3.219.124, 80.3.219.124, 48.107.36.30, 48.107.36.30, 24.41.56.94, 24.41.56.94, 24.41.56.94, 24.41.56.94, 245.41.149.165, 245.41.149.165, 130.127.118.52, 130.127.118.52, 130.127.118.52, 130.127.118.52, 7.233.31.155, 7.233.31.155, 167.33.184.64, 167.33.184.64, 22...
```

## Example 4

```{sh}
# Top 50 users according to the number of IPs they connected from (transformation map->transformation sortByKey->transformation map)
scala> val top = userIPs.map(v=>(v._2.toSeq.length, v._1)).sortByKey(false).map(item=>item.swap)
scala> top.take(50)
res: Array[(String, Int)] = Array((152,856), (96,850), (46,846), (40,842), (203,836), (192,832), (160,830), (126,818), (189,814), (130,812), (85,812), (198,812), (62,811), (145,808), (197,808), (67,804), (148,804), (9,804), (119,804), (5,802), (208,794), (15,792), (109,790), (151,790), (76,788), (90,786), (122,786), (146,784), (207,782), (100,782), (14,782), (194,782), (32,780), (142,780), (94,780), (30,778), (56,778), (138,776), (187,774), (73,772), (7,772), (190,772), (196,772), (69,770), (205,770), (164,770), (36,768), (140,768), (75,766), (87,766))
scala> :quit
```
