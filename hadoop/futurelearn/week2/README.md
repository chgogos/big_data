# Week2

## 2.3 Basic command in Hadoop

    ```sh
    $ start-dfs.sh
    $ start-yarn.sh
    $ hadoop fs -ls
    $ hadoop -copyFromLocal <source-path> <dest-path>
    ```

## 2.7 Hadoop + AWK

* [Term_frequencies_sentence-level_lemmatized_utf8.csv](https://bscw.lecad.fs.uni-lj.si/pub/bscw.cgi/d226569/Term_frequencies_sentence-level_lemmatized_utf8.csv)

    ```sh
    $ hadoop fs -cat Term_frequencies_sentence-level_lemmatized_utf8.csv | awk -F ";" '{print $2 ";" $3 ";" $4}' | hadoop fs -put - results.txt
    ```

* AWK script name: awkMaximumNumber

    ```awk
    BEGIN {
	max1=0;
	max2=0;
	max3=0;
	FS =";";
	print "Initial Maximal values set";
    }
    {
        if($1>max1){max1=$1;};
        if($2>max2){max2=$2};
        if($3>max3){max3=$3};
    }
    END{
        print "First maximum is "max1;
        print "Second maximum is "max2;
        print "Third maximum is "max3;
    }
    ```

* Find the maximal numbers in columns 2, 3 and 4
    
    ```sh
    $ chmod +x awkMaximumNumber
    $ hadoop fs -cat Term_frequencies_sentence-level_lemmatized_utf8.csv | awk -F ";" '{print $2 ";" $3 ";" $4}' | awk -f  /home/hduser/awkMaximumNumber
    ```

## 2.13 Use Map and Reduce to find the most frequent words in text columns (python)

* mapperExampleMook.py

    ```python
    #!/usr/bin/env python

    import sys

    # input comes from STDIN (standard input)
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()
        # split the line into words
        words = line.split(';')
        # increase counters
        if len(words) != 5:
        continue # Removing error data
        
        for i in range(1,4):
        print '%d\t%d\t%s' % (i,int(words[i]), words[0])
    ```

* Testing the mapper

    ```sh
    $ cat ./week2/Term_frequencies_sentence-level_lemmatized_utf8.csv | python mapperExampleMook.py
    ```

* reducerExampleMook.py

    ```python
    #!/usr/bin/env python

    import sys

    def getKey(item):
    return item[0]

    def print_res(max_nums, NF):
    str_list = sorted(max_nums, key=getKey, reverse=True)

    for i in range(0,min(len(str_list),NF)):
        print '%d\t%d\t%s' % (current_map_key, str_list[i][0], str_list[i][1])


    current_map_key = None 
    word = None
    NF=10

    max_nums = []

    # input comes from STDIN
    for line in sys.stdin:
        # remove leading and trailing whitespace
        line = line.strip()

        # parse the input we got from mapper.py
        mapper_key, count, word = line.split('\t', 3)

        # convert count (currently a string) to int
        try:
            count = int(count)
            mapper_key = int(mapper_key)
        except ValueError:
            # count was not a number, so silently
            # ignore/discard this line
            continue

        if current_map_key is None:
        current_map_key = mapper_key
        elif current_map_key != mapper_key:
        #we received key that is different than the initial, so ignore
        print_res(max_nums, NF)
        print 'Change'
        max_nums = []
        current_map_key = mapper_key
        

        max_nums.append((count,word))

    print_res(max_nums, NF)
    ```

* Testing mapper and reducer locally

    ```sh
    $ cat Term_frequencies_sentence-level_lemmatized_utf8.csv | sort | python mapper.py | python reducer.py
    ```

* Run mapper and reducer on Hadoop

    ```sh
    $ hadoop jar /usr/local/hadoop-2.6.5/share/hadoop/tools/lib/hadoop-streaming-2.6.5.jar -D mapred.reduce.tasks=2 -mapper "mapperExampleMook.py" -reducer "reducerExampleMook.py" -input Term_frequencies_sentence-level_lemmatized_utf8.csv -output ./gutenberg-output_count -file /home/hduser/Desktop/mapperExampleMook.py -file /home/hduser/Desktop/reducerExampleMook.py

    $ hadoop fs -cat /user/hduser/gutenberg-output_count/part-00000
    ...
    $ hadoop fs -cat /user/hduser/gutenberg-output_count/part-00001
    ...
    ```