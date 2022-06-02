# Not everything is Big Data

* [Command-line Tools can be 235x Faster than your Hadoop Cluster](https://adamdrake.com/command-line-tools-can-be-235x-faster-than-your-hadoop-cluster.html)
* Amazon EMR (Elastic Map Reduce) 7 c1.medium machines took 26 minutes to process 1.75GB while laptop, using shell commands took only 12 seconds
  * c1.medium <https://aws.amazon.com/ec2/previous-generation/>


*Download chess game data (pgn files) from <https://github.com/rozim/ChessData>

    ```sh
    # assuming that chessData/Filtered is under the current folder
    $ cd chessData/Filtered
    $ head filtered_00.pgn
    [Event "BL 0708 SK Zehlendorf - OSC Baden Baden"]
    [Site "?"]
    [Date "2007.10.20"]
    [Round "1.1"]
    [White "Svidler, Peter"]
    [Black "Maksimenko, Andrei"]
    [Result "1-0"]
    [WhiteElo "2735"]
    [BlackElo "2505"]
    [ECO "C63"]
    ```

    ```sh
    $ du -h
    1.4G  .
    # example using 1.4G of chess data
    $ cat *.pgn | grep 'Result' | sort | uniq -c
    620712 [Result "0-1"]
    830032 [Result "1-0"]
    738352 [Result "1/2-1/2"]
    $ time cat *.pgn | grep 'Result' | sort | uniq -c
    620712 [Result "0-1"]
    830032 [Result "1-0"]
    738352 [Result "1/2-1/2"]
    real 0m27.995s
    user 0m28.954s
    sys 0m1.977s
    # even better results using awk and xargs
    $ time find . -type f -name '*.pgn' -print0 | xargs -0 -n 4 -P 0 mawk '/Result/ { split($0, a, "-"); res = substr(a[1], length(a[1]), 1); if (res == 1) white++; if (res == 0) black++; if (res == 2) draw++ } END { print white+black+draw, white, black, draw }' | mawk '{games += $1; white += $2; black += $3; draw += $4; } END { print games, white, black, draw }'
    2189096 830032 620712 738352
    real	0m2.666s
    user	0m7.823s
    sys	0m0.862s
    ```

Cited conclusion from <https://adamdrake.com/command-line-tools-can-be-235x-faster-than-your-hadoop-cluster.html>

Hopefully this has illustrated some points about using and abusing tools like Hadoop for data processing tasks that can better be accomplished on a single machine with simple shell commands and tools. If you have a huge amount of data or really need distributed processing, then tools like Hadoop may be required, but more often than not these days I see Hadoop used where a traditional relational database or other solutions would be far better in terms of performance, cost of implementation, and ongoing maintenance.


