# Not everything is Big Data

[Command-line Tools can be 235x Faster than your Hadoop Cluster](https://adamdrake.com/command-line-tools-can-be-235x-faster-than-your-hadoop-cluster.html)

    Amazon EMR (Elastic Map Reduce)
    7 c1.medium <https://aws.amazon.com/ec2/previous-generation/> machine in the cluster took 26 minutes to process 1.75GB

    vs

    Laptop, using shell commands took 12 seconds to process 1.75GB

Download chess game data (pgn files) from <https://github.com/rozim/ChessData>

    # assuming that chessData\Filtered is under the current folder
    $ cd chessData\Filtered
    $ head filtered_00.pgn
    [Event "BL 0708 SK Zehlendorf - OSC Baden Baden"]
    [Site "?"]
    [Date "2007.10.20"]
    [Round "1.1"]
    [White "Svidler, Peter"]
    [Black "Maksimenko, Andrei"]
    [Result "1-0"]
    [WhiteElo "2735"]
    [BlackElo "2

    $ du -h
    1.4G  .
    # example using 1.4G of chess data
    $ cat *.pgn | grep 'Result' | sort | uniq -c
    620712 [Result "0-1"]
    830032 [Result "1-0"]
    738352 [Result "1/2-1/2"]
    $ time cat *.pgn | grep 'Result' | sort | uniq -c
    ...
    real 0m27.995s
    user 0m28.954s
    sys 0m1.977s