# Week 3

# 3.7 Setting up the RHadoop working place

Start Hadoop

    ```sh
    $ start-dfs.sh
    $ start-yarn.sh
    $ rstudio &
    ```

RHadooop

    ```{r}
    Sys.setenv(HADOOP_OPTS="-Djava.library.path=/usr/local/hadoop/lib/native")
    Sys.setenv(HADOOP_HOME="/usr/local/hadoop")
    Sys.setenv(HADOOP_CMD="/usr/local/hadoop/bin/hadoop")
    Sys.setenv(HADOOP_STREAMING="/usr/local/hadoop/share/hadoop/tools/lib/hadoop-streaming-2.6.5.jar")
    Sys.setenv(JAVA_HOME="/usr/lib/jvm/java-8-openjdk-amd64")

    library(rhdfs)
    library(rmr2)

    hdfs.init()
    ```

Stop Hadoop

    ```sh
    $ stop-dfs.sh
    $ stop-yarn.sh
    ```

# 3.8 First Big Data example with RHadoop

## Synthetic data

    ```{r}
    N=1000
    X=rbinom(N,5,0.5)
    Y=sample(c("a","b","c","d","e"), N, replace=TRUE)
    Data=data.frame(X,Y)
    colnames(Data)=c("value","group")

    # store data to the distributed file system
    to.dfs(Data, "SmallData",format="native")

    # retrieve data from the distributed file system
    SmallData=from.dfs("/SmallData")

    # delete data from the distributed file system
    dfs.rmr("/SmallData")
    ```

## CEnetBig example

    ```sh
    $ hadoop fs -ls /
    $ hdfs fsck /CEnetBig
    ```

Count how many customers have a  particular product (out of 5 possible)

    ```{r}
    CEnetBig<-from.dfs("/CEnetBig")
    dim(CEnetBig$val)
    
    # without MR
    T = table(CEnetBig$val[,"type"]);T

    # mapper
    mapperTable = function (., X) {
        T=table(X[,"type"])
        keyval(1,list(T))
    }

    # reducer
    reducerTable = function(k, A) {
        keyval(k,list(Reduce('+', A)))
    }

    # map-reducer code
    BigTable<-from.dfs(
        mapreduce(
            input = "/CEnetBig",
            map = mapperTable,
            reduce = reducerTable
        )
    )

    # display result
    BigTable$val
    [[1]]

        1      2      3      4      5 
    199857 250569  99595 299849 150130
    ```