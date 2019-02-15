# HADOOP - MAPREDUCE - HADOOP streaming

Udacity + cloudera MOOC <https://www.udacity.com/course/intro-to-hadoop-and-mapreduce--ud617>

   Udacity Hadoop VM
   user: training
   password: training

## Map Reduce example 1 (MR_example1)

(Udacity course)

Purchase data beginning at 1/1/2012 and ending at 31/12/2012  (4138476 lines)

Data (purchases.txt) can be downloaded from <http://content.udacity-data.com/courses/ud617/purchases.txt.gz>

    # Test locally
    $ more purchases.txt
    2012-01-01      09:00   San Jose        Men's Clothing  214.05  Amex
    2012-01-01      09:00   Fort Worth      Women's Clothing        153.57  Visa
    ...
    $ wc -l purchases.txt
    4138476 purchases.txt
    $ head -50 purchases.txt > pur50.txt
    $ cat pur50.txt | ./mapper.py | sort | ./reducer.py
    Anchorage 327.6
    Aurora 117.81
    ...
    $ cat purchases.txt | ./mapper.py | sort | ./reducer.py
    Albuquerque 10052311.42
    Anaheim 10076416.36
    ...
    $ time cat purchases.txt | ./mapper.py | sort | ./reducer.py
    ...
    real 0m25.061s
    user 0m29.226s
    sys 0m1.113s

    # Test on Hadoop (virtual) cluster
    # localhost:50070  --> name node
    # localhost:50030  --> job tracker
    VM$ cd udacity_training
    VM$ tree
    VM$ hadoop fs -ls
    # copy data from local storage to the Hadoop cluster
    VM$ hadoop fs -mkdir myinput
    # no navigation is possible in hdfs with cd, full paths are needed
    # upload data to hdfs
    VM$ hadoop fs -put /data/purchases.txt myinput
    VM$ hadoop fs -ls -h myinput
    # execute map reduce job using alias hs
    VM$ hs code/mapper.py code/reducer.py myinput output1
    # execute map reduce job with full command using jar
    VM$ hadoop jar /usr/lib/hadoop-0.20-mapreduce/contrib/streaming hadoop-streaming-2.0.0-mr1-cdh4.1.1.jar -mapper mapper.py -reducer reducer.py -file mapper.py -file reducer.py -input myinput -output output1
    # display results
    VM$ hadoop fs -cat output1/part-00000
    # copy results from the Hadoop cluster to the local machine
    VM$ hadoop fs -get output1/part-00000 results.txt
    VM$ more results.txt
    # delete input and output data from the Hadoop cluster
    VM$ hadoop fs -rm -r -f myinput
    VM$ hadoop fs -rm -r -f output1

## Map Reduce example 2 (MR_example2)

Web log processing example. Count number of hits at each web page resource

Data (access_log.gz) can be downloaded from <http://content.udacity-data.com/courses/ud617/access_log.gz>.

    # test locally
    $ gunzip access_log.gz
    # create a small subset of data for testing
    $ head -50 access_log > log50.txt
    $ cat log50.txt | ./mapper.py | sort | ./reducer.py
    $ cat access_log | ./mapper.py | sort | ./reducer.py > results.txt
    # sort based on 2nd column, treat values of the 2nd column as numbers
    $ sort -k 2n results.txt

<!-- 
	// connect from the host (computer that hosts the VM) to the VM
	$ ssh training@XXX.XXX.XXX.XXX
	// copy data from the VM to the host
	$ scp training@XXX.XXX.XXX.XXX:/home/training/udacity_training/data/purchases.txt . -->
