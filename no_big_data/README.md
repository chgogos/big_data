# Not everything is Big Data

Command-line Tools can be 235x Faster than your Hadoop Cluster <https://adamdrake.com/command-line-tools-can-be-235x-faster-than-your-hadoop-cluster.html>

	Amazon EMR (Elastic Map Reduce)
	7 c1.medium machine in the cluster took 26 minutes to process 1.75GB

	vs 

	Laptop, using shell commands took 12 seconds to process 1.75GB

Download chess game data from <https://github.com/rozim/ChessData>

# Example

	// assuming that chessData is under the current folder
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

	$ du -h // disk usage
	1.4G  .
	$ cat *.pgn | grep 'Result' | sort | uniq -c 
	620712 [Result "0-1"]
	830032 [Result "1-0"]
	738352 [Result "1/2-1/2"]
