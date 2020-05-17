# Data manipulation at scale MR examples

## Problem 0 (word count problem using data in a json file)

* [mr0.py](./mr0.py)
* [text1.json](./text1.json)
* [text2.json](./text2.json)

```{sh}
$ python mr0.py text1.json
MAP PHASE
key=aaa value=1
key=bbb value=1
key=ccc value=1
key=aaa value=1
key=bbb value=1
key=eee value=1
key=aaa value=1
key=bbb value=1
key=fff value=1
key=aaa value=1
key=eee value=1
key=fff value=1
SHUFFLE PHASE
key=eee value=[1, 1]
key=fff value=[1, 1]
key=aaa value=[1, 1, 1, 1]
key=bbb value=[1, 1, 1]
key=ccc value=[1]
REDUCE
["eee", 2]
["fff", 2]
["aaa", 4]
["bbb", 3]
["ccc", 1]
$ python mr0.py text2.json 
```

## Problem 1 (inverted index for books)

* [mr1.py](./mr1.py)
* [text1.json](./text1.json)
* [books.json](./books.json)

```{sh}
$ python mr1.py text1.json
$ python mr1.py books.json 
```

## Problem 2 (join Orders with LineItems)

* [mr2.py](./mr2.py)
* [records.json](./records.json)

```{sh}
$ python mr2.py records.json
```

## Problem 3 (count friends of each person in a social network)

* [mr3.py](./mr3.py)
* [friends1.json](./friends1.json)
* [friends.json](./friends.json)

```{sh}
$ python mr3.py friends1.json 
$ python mr3.py friends.json 
```

## Problem 4 (reveal asymmetric friendship relationships)

* [mr4.py](./mr4.py)
* [friends1.json](./friends1.json)
* [friends.json](./friends.json)

```{sh}  
$ python mr4.py friends1.json 
$ python mr4.py friends.json
```

## Problem 5 (DNA example)

* [mr5.py](./mr5.py)
* [dna1.json](./dna1.json)
* [dna.json](./dna.json)

```{sh}
$ python mr5.py dna1.json
$ python mr5.py dna.json
```

## Problem 6 (MATRIX multiplication)

* [mr6.py](./mr6.py)
* [matrix1.json](./matrix1.json)
* [matrix.json](./matrix.json)

```{sh}
python mr6.py matrix1.json     (A=2x3, B=3x2)
python mr6.py matrix.json      (A=5x5, B=5x5)
```

## Problem 6 (MATRIX multiplication - β' τρόπος)

* [mr6b.py](./mr6b.py)
* [matrix1.json](./matrix1.json)
* [matrix.json](./matrix.json)

```{sh}
python mr6b.py matrix1.json     (A=2x3, B=3x2)
python mr6b.py matrix.json      (A=5x5, B=5x5)
```
