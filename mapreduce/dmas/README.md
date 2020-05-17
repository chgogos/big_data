# Data manipulation at scale MR examples

## Problem 0 (word count problem using data in a json file)

* [mr0.py](./mr0.py)
* [text1.json](./text1.json)
* [text.json](./text.json)

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
$ python mr0.py text.json
["passages", 1]
["don't", 1]
["true", 1]
["text", 2]
["over", 1]
["random", 1]
["Internet.", 1]
["including", 1]
["looks", 1]
["Internet", 1]
["1500s,", 1]
["its", 1]
["looking", 1]
["generator", 1]
["majority", 1]
["believable.", 1]
["sentence", 1]
["desktop", 1]
["to", 6]
... 
```

## Problem 1 (inverted index for books)

* [mr1.py](./mr1.py)
* [books1.json](./books1.json)
* [books.json](./books.json)

```{sh}
$ python mr1.py books1.json
["eee", ["text2", "text4"]]
["fff", ["text3", "text4"]]
["aaa", ["text1", "text2", "text3", "text4"]]
["bbb", ["text1", "text2", "text3"]]
["ccc", ["text1"]]
$ python mr1.py books.json
["all", ["milton-paradise.txt", "blake-poems.txt", "melville-moby_dick.txt"]]
["Rossmore", ["edgeworth-parents.txt"]]
["Consumptive", ["melville-moby_dick.txt"]]
["forbidden", ["milton-paradise.txt"]]
["child", ["blake-poems.txt"]]
["eldest", ["edgeworth-parents.txt"]]
["four", ["edgeworth-parents.txt"]]
["Caesar", ["shakespeare-caesar.txt"]]
["winds", ["whitman-leaves.txt"]]
["Moses", ["bible-kjv.txt"]]
["children", ["edgeworth-parents.txt"]]
["seemed", ["austen-emma.txt", "chesterton-ball.txt"]]
... 
```

## Problem 2 (join Orders with LineItems)

* [mr2.py](./mr2.py)
* [records.json](./records.json)

```{sh}
$ python mr2.py records.json
["order", "32", "130057", "O", "208660.75", "1995-07-16", "2-HIGH", "Clerk#000000616", "0", "ise blithely bold, regular requests. quickly unusual dep", "line_item", "32", "82704", "7721", "1", "28", "47227.60", "0.05", "0.08", "N", "O", "1995-10-23", "1995-08-27", "1995-10-26", "TAKE BACK RETURN", "TRUCK", "sleep quickly. req"]
["order", "32", "130057", "O", "208660.75", "1995-07-16", "2-HIGH", "Clerk#000000616", "0", "ise blithely bold, regular requests. quickly unusual dep", "line_item", "32", "197921", "441", "2", "32", "64605.44", "0.02", "0.00", "N", "O", "1995-08-14", "1995-10-07", "1995-08-27", "COLLECT COD", "AIR", "lithely regular deposits. fluffily "]
...
```

## Problem 3 (count friends of each person in a social network)

* [mr3.py](./mr3.py)
* [friends1.json](./friends1.json)
* [friends.json](./friends.json)

```{sh}
$ python mr3.py friends1.json
["A", 2]
["C", 1]
["B", 2] 
$ python mr3.py friends.json
["MlleBaptistine", 3]
["Myriel", 5]
["Valjean", 16]
["MmeMagloire", 1]
["Champtercier", 1]
["Napoleon", 1] 
```

## Problem 4 (reveal asymmetric friendship relationships)

* [mr4.py](./mr4.py)
* [friends1.json](./friends1.json)
* [friends.json](./friends.json)

```{sh}  
$ python mr4.py friends1.json
"For A relationship with C is asymmetric"
"For C relationship with A is asymmetric" 
$ python mr4.py friends.json
"For MlleBaptistine relationship with Myriel is asymmetric"
"For MlleBaptistine relationship with Valjean is asymmetric"
"For MlleBaptistine relationship with MmeMagloire is asymmetric"
"For Fantine relationship with Valjean is asymmetric"
"For Cosette relationship with Valjean is asymmetric"
"For Babet relationship with Valjean is asymmetric"
"For Valjean relationship with MlleBaptistine is asymmetric"
"For Valjean relationship with MmeMagloire is asymmetric"
"For Valjean relationship with Labarre is asymmetric"
"For Valjean relationship with Marguerite is asymmetric"
"For Valjean relationship with MmeDeR is asymmetric"
"For Valjean relationship with Isabeau is asymmetric"
"For Valjean relationship with Fantine is asymmetric"
"For Valjean relationship with Cosette is asymmetric"
"For Valjean relationship with Simplice is asymmetric"
"For Valjean relationship with Woman1 is asymmetric"
"For Valjean relationship with Judge is asymmetric"
"For Valjean relationship with Woman2 is asymmetric"
"For Valjean relationship with Gillenormand is asymmetric"
"For Valjean relationship with MlleGillenormand is asymmetric"
"For Valjean relationship with Babet is asymmetric"
"For Valjean relationship with Montparnasse is asymmetric"
"For MmeMagloire relationship with MlleBaptistine is asymmetric"
"For MmeMagloire relationship with Myriel is asymmetric"
"For MmeMagloire relationship with Valjean is asymmetric"
"For Labarre relationship with Valjean is asymmetric"
"For Montparnasse relationship with Valjean is asymmetric"
"For Judge relationship with Valjean is asymmetric"
"For Napoleon relationship with Myriel is asymmetric"
"For Count relationship with Myriel is asymmetric"
"For Isabeau relationship with Valjean is asymmetric"
"For Woman1 relationship with Valjean is asymmetric"
"For Woman2 relationship with Valjean is asymmetric"
"For Simplice relationship with Valjean is asymmetric"
"For Geborand relationship with Myriel is asymmetric"
"For Gillenormand relationship with Valjean is asymmetric"
"For Marguerite relationship with Valjean is asymmetric"
"For Myriel relationship with Geborand is asymmetric"
"For Myriel relationship with Count is asymmetric"
"For Myriel relationship with OldMan is asymmetric"
"For Myriel relationship with Napoleon is asymmetric"
"For Myriel relationship with MlleBaptistine is asymmetric"
"For Myriel relationship with MmeMagloire is asymmetric"
"For OldMan relationship with Myriel is asymmetric"
"For MmeDeR relationship with Valjean is asymmetric"
"For MlleGillenormand relationship with Valjean is asymmetric"
```

## Problem 5 (DNA example)

* [mr5.py](./mr5.py)
* [dna1.json](./dna1.json)
* [dna.json](./dna.json)

```{sh}
$ python mr5.py dna1.json
"GG"
"AA"
"GA"
$ python mr5.py dna.json
"CTGCAGCCACCCCCTGCTGCCCCCACCTGAACCCTTGATCCCAGCTCGGCAGCCCCCGCAGTTTCCTGTTTGCCCACTCTCTTTGCCCAGCCTCAGGAACAGAGCTGATCCTTGAACTCTAAGTTCCACATCGCCAGCAAAAGTAAGCAGTGGCAGGGCCAGGCTGAGCTTATCAGTCTCCCAAGTCCCCAGCCCCTGCCCACACACATATATAGACCAGGGAAGAAGAGCTGGACACCCAGACTGTCGGAGAGCTCCGGGGAGGTAAGTGCTGCTACCTGCCTTCGGTGGCTCTGGCTCCATAGCGCCTCCCAGTTGATGCTCCACTGTCCAAAGTCCAAATCAATACCGTTGGATATCTCGCACCTTTAGCCATTCTAGCCAATGCTTCCATGGGCTTGAATTGTGTGTGGAGCCTTTCCATGACAATGCCTCCCTTCAGCCCAGCCCTCCTCCTCCTTCTTCTTCTTCAGGTCACCCACACCCTTCAGGATGAAAGCTGTGGTGCTGGCCGTGGCTCTGGTCTTCCTGACAGGTAGGTGCCTCTTGACCTGCGTGGGACTACCTTCCTGGGCACAGAGAACAGAATTCCCACTGTTCTCTTCCCTGACTCCGAGTCTAACCTAACATGGGTCTCCCCTCCATCCCCAGGGAGCCAGGCTTGGCACGTATGGCAGCAAGATGAACCCCAGTCCCAATGGGACAAAGTGAAGGATTTCGCTAATGTGTATGTGGATGCGGTCAAAGACAGCGGCAGAGACTATGTGTCCCAGTTTGAATCCTCCTCCTTGGGCCAACAGCTGAAGTAAGGAAAGACCTAGCGTGGGGCCTGGAGCAGGTCAAGGGCTGCCCATCCAGGGTGGGCAGAGAGACCAGTGAGAAGATGCTGGAACTGAGCTGGCTAGCCCTTCACGGGCTTTCCTACCAGCTGGGCACCATGGCAGGTTCCAGTGGAGGACTAGGGATGGGATTCATCTGGCTGTTGGGTAACCACAGCCCCTCATTCAGCCTATGAGTGCCAAATCCCTTTTCCTTGGTAACCCCCAGTACTGGTCAGACAGCACCCAAAACAAAACAAAACAAAACAAAAAACAAAAAACGGGACTGGCCTTGTAACCAGCACCGACCACATCTGTGTTCTTGGTGCCTTCCGCCATGTGTACAGAGGAATGTAAAGGGAAGGCAGTGAGTTAGAGGGTTGCATGTTCGGGGAAACTAGGACCATAGCAACTGCACATTAGGGGACAGGTGGCACCCAGCTATCATGTGCATGGATCTGCAGACCAGGGGCAGCGCATGATGCCTGGGCTCGTCTCTCAGCCGCTCTCTTCCCCCTCTAGCCTGAATCTCCTGGAAAACTGGGACACTCTGGGTTCAACCGTTAGTCAGCTGCAGGAACGGCTGGGCCCATTGACTCGGGACTTCTGGGATAACCTGGAGAAAGAAACAGATTGGGTGAGACAGGAGATGAACAAGGACCTAGAGGAAGTGAAACAGAAGGTGCAGCCCTACCTGGACGAATTCCAGAAGAAATGGAAAGAGGATGTGGAGCTCTACCGCCAGAAGGTGGCGCCTCTGGGCGCCGAGCTGCAGGAGAGCGCGCGCCAGAAGCTGCAGGAGCTGCAAGGGAGACTGTCCCCTGTGGCTGAGGAATTTCGCGACCGCATGCGCACACACGTAGACTCTCTGCGCACACAGCTAGCGCCCCACAGCGAACAGATGCGCGAGAGCCTGGCCCAGCGCCTGGCTGAGCTCAAGAGCAACCCTACCTTGAACGAGTACCACACCAGGGCCAAAACCCACCTGAAGACACTTGGCGAGAAAGCCAGACCTGCGCTGGAGGACCTGCGCCATAGTCTGATGCCCATGCTGGAGACGCTTAAGACCAAAGCCCAGAGTGTGATCGACAAGGCCAGCGAGACTCTGACTGCCCAGTGAGGTGCCCGCTTCCACTCCCCACCCCCGCATTGGCTTTCTTACAATAAACCTTTCCAAAATGGAATAGCTTCTTTCTTTGGGGGACATAGGGCGGGCGCTAAGGGGACATCAAGGGACGTGAGAACATGGTGCCGCACTGGGGATTCCTTTGTACGCGACATCTCAGCTCTTAACGCTCACTCAAGCTGGGCACCTGGCTGGTTCAGGGTATGAGACAGAATCCTTCTA"
"CCATGGGTTGGCCAGCCTTGCCTTGACCAATAGCTTTGACAAGGCAACCTTGACCAATAGTCTTAGAGTATCGGGTGAGGCCCGGGGGCCGGTGGGTGGCTAGGGATGAAGAATAAAAGGAAGCACCCTCCATCAGTTCCACATACTCGCTCTGAAACGTCTGAGATTATCAATAAGCTCCTTGTCCAGACGCCATGAGTAATTTCACAGCTGAGGACAAGGCTGCTATCACTAGCCTGTGGGGCAAGGTGAATGTGGAAGATGCTGGGGGAGAAACCCTGGGAAGGTAGGCTCTGGTGACCAGGACGAGGGAGGGAAGGAAGGAACCTATGCCTGGCAAAAGTTCAGGCTGCCTCTCAGGATTTGTGGCACCTTCTGACTTTCAAACTGCTATTGTTCAATCTCACAGGCTCCTGGTTGTGTACCCATGGACCCAGAGGTTCTTCGACAGCTTTGGAAGCCTGTCCTCTCCCTCTGCCATCATGGGCAACCCCAAAGTCAAGGCGCATGGCGTGAAGGTGCTGACTTCCTTGGGAGAAGCTATAAAGAACCTTGATGATCTCAAGGGCACCTTTGGCCAGCTGAGTGAGCTGCACTGTGACAAGCTGCATGTGGATCCTGAGAACTTCAGGGTGAGTCCAGGAGATATTGGGGTTGGGAGTTAAGAAACTTCAGAGGACTACCTGGGCTGAGACCCAGTGGTAATGTTTTAGGGCCTACGGAGTGCCTCTAAAAATCGAGAGGGACAACTTTGGCTTCGAGAAAAGAGTTGTGGAAACGAGGACAATGACTTTTCTTTATTAGAGTCTGGTAGAAAGAACTTTATCTTTCCCTCATTTTGATTATCTATTTAAAACATCTATCTGAAAGCAGGACAAGTATGGCCATTAAAAAGATGCAGGCAGAGGCATATATTGGCTCCATCCAAGTGGAGAACTTTGGTGGCCAAACATATATTGCTAAGGCTATTCCTGTAATTAGCTGGACACATACAAAATGCTGCAAATGCTTCATTATAAACTTACATCCTATAATTCCAAATGGGGCAAAAGTGTTTCTGGGGGTGAGAAAGAATAGAAACATTTGTCCTGGAGTAGATTTTTTAGTCAGTTGCGAGTGTGTGTATGTATGTGTGTTTTTTTTGTGTGTGTGTGCGAGCATGTGTTTCTTTTAAAGTTTTCAGCCTACAAAATACAGGGTTTGTGGTAGCAAGAAGATAGCTAGATTTAAATTATGCCAGTGACTAATGCTGCAAGGGGAACAGCTACCTGCATTTAATGGGTAGGCAAAATCCAGGCTTTGAGGGAAGTTAACATAGGCTTGATTCCGGGTGGAAGGTGGGTGTGTAGTTATCCAGAGGCCAGGCTGGAGCCCTCTGTTCACTATGGGTTTATATTTGTTGTCCCCTTTCAACTCAACAGCTCCTGGGAAATGTGCTGGTGACTGTTTTGGCAATCCTTCATGGCAAAGAATTCACCCCTGAGGTGCAGGCTTCCTGGCAGAAGATGGTGGCTGGAGTGGCCAGTGCCTTGGCCTCCAGATACCACTGAAGCCCCTGCCCATGATGCAGAGCTTTCAAGGAGTGGCTTTATTCCGCAAGCAATAAAAATAATAAAACTATTCCGCTCAAAGATCACACGTGATTGTCGTCAGTTATTTTTTCCTTGTCCTTCCAAATATGCGAACCACAAAGGG"
...
```

## Problem 6 (MATRIX multiplication)

* [mr6.py](./mr6.py)
* [matrix1.json](./matrix1.json)
* [matrix.json](./matrix.json)

```{sh}
# (A=2x3, B=3x2)
$ python mr6.py matrix1.json 2 3 2   
[[0, 1], 14]
[[1, 0], 19]
[[0, 0], 5]
[[1, 1], 28]
# (A=5x5, B=5x5)
$ python mr6.py matrix.json 5 5 5    
[[1, 3], 7479]
[[3, 0], 10512]
[[2, 1], 9880]
[[0, 3], 5964]
[[4, 0], 11182]
[[1, 2], 8282]
[[3, 3], 2934]
[[4, 4], 9981]
[[2, 2], 10636]
[[4, 1], 14591]
[[1, 1], 6914]
[[3, 2], 10587]
[[0, 0], 11878]
[[0, 4], 15874]
[[1, 4], 9647]
[[2, 3], 6973]
[[4, 2], 10954]
[[1, 0], 4081]
[[0, 1], 14044]
[[3, 1], 12037]
[[2, 4], 8873]
[[2, 0], 6844]
[[4, 3], 1660]
[[3, 4], 5274]
[[0, 2], 16031]
```

## Problem 6 (MATRIX multiplication - alternative implementation using dictionaries)

* [mr6b.py](./mr6b.py)
* [matrix1.json](./matrix1.json)
* [matrix.json](./matrix.json)

```{sh}
# (A=2x3, B=3x2)
$ python mr6b.py matrix1.json 2 3 2   
[[0, 1], 14]
[[1, 0], 19]
[[0, 0], 5]
[[1, 1], 28]

# (A=5x5, B=5x5)
$ python mr6b.py matrix.json 5 5 5    
[[1, 3], 7479]
[[3, 0], 10512]
[[2, 1], 9880]
[[0, 3], 5964]
[[4, 0], 11182]
[[1, 2], 8282]
[[3, 3], 2934]
[[4, 4], 9981]
[[2, 2], 10636]
[[4, 1], 14591]
[[1, 1], 6914]
[[3, 2], 10587]
[[0, 0], 11878]
[[0, 4], 15874]
[[1, 4], 9647]
[[2, 3], 6973]
[[4, 2], 10954]
[[1, 0], 4081]
[[0, 1], 14044]
[[3, 1], 12037]
[[2, 4], 8873]
[[2, 0], 6844]
[[4, 3], 1660]
[[3, 4], 5274]
[[0, 2], 16031]
```
