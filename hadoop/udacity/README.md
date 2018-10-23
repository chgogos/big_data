# HADOOP - MAPREDUCE - HADOOP streaming

	Udacity + cloudera MOOC
	https://www.udacity.com/course/intro-to-hadoop-and-mapreduce--ud617
	Udacity Hadoop VM
	user: training
	password: training

	// hdfs 
	VM$ pwd
	VM$ ls
	VM$ cd udacity_training
	VM$ tree
	VM$ cd data
	VM$ ls -lh
	VM$ ls -l --block-size=M
	VM$ head purchases.txt
	VM$ more purchases.txt
	VM$ less purchases.txt
	VM$ wc -l purchases.txt
	VM$ hadoop fs -ls
	VM$ hadoop fs -ls -h
	VM$ hadoop fs -put purchases.txt 
	VM$ hadoop fs -tail purchases.txt
	VM$ hadoop fs -mv purchases.txt newname.txt
	VM$ hadoop fs -ls
	VM$ hadoop fs -rm newname.txt
	VM$ hadoop fs -mkdir myinput
	VM$ hadoop fs -put purchases.txt myinput
	VM$ hadoop fs -ls myinput
	VM$ hadoop fs -rm -f -r myinput 
	// no navigation is possible in hdfs with cd, full paths are needed

	// udacity code
	VM$ cd ../code
	VM$ gedit mapper.py &
	VM$ gedit reducer.py &

	// execute code locally
	VM$ head -50 ../data/purchases.txt > testfile
	VM$ cat testfile | ./mapper.py
	VM$ cat testfile | ./mapper.py | sort | ./reducer.py

	// namenode 
	http://localhost:8020  // name node
	    Browse the filesystem > user > training

	// execute MapReduce job
	http://localhost:50030  // job tracker
	VM$ hadoop jar 
	    /usr/lib/hadoop-0.20-mapreduce/contrib/streaming/hadoop-streaming-2.0.0-mr1-cdh4.1.1.jar 
	    -mapper mapper.py -reducer reducer.py 
	    -file mapper.py -file reducer.py 
	    -input myinput -output output1

	// alias hs for the previous command
	VM$ hs mapper.py reducer.py myinput output1

	// copy data from hdfs to VM local
	VM$ hadoop fs -ls
	VM$ hadoop fs -get output1/part-00000 results.txt
	VM$ cat results.txt

	// connect from the host (computer that hosts the VM) to the VM
	$ ssh training@XXX.XXX.XXX.XXX
	// copy data from the VM to the host
	$ scp training@XXX.XXX.XXX.XXX:/home/training/udacity_training/data/purchases.txt .
