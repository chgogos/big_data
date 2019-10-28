import MapReduce
import sys

"""
MATRIX multiplication
A is L x M
B is M x N
L=M=N=5
"""

# matrix.json (A=5x5, B=5x5)
# L = 5
# M = 5
# N = 5

# matrix1.json (A=2x3, B=3x2)
# L = 2
# M = 3
# N = 2

mr = MapReduce.MapReduce()


def mapper(record):
    key = record[0]
    i = record[1]
    j = record[2]
    v = record[3]
    if record[0] == "a":
        for k in range(N):
            mr.emit_intermediate((i, k), ('a', i, j, v))

    if record[0] == "b":
        for k in range(L):
            mr.emit_intermediate((k, j), ('b', i, j, v))


def reducer(key, list_of_values):
    sum = 0
    for v1 in list_of_values:
        if v1[0] == "a":
            for v2 in list_of_values:
                if v2[0] == "b" and v1[2] == v2[1]:
                    sum += v1[3]*v2[3]
    mr.emit((key, sum))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)