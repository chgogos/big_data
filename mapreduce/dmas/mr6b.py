import MapReduce
import sys

"""
MATRIX multiplication
A is L x M
B is M x N
"""

# matrix.json (A=5x5, B=5x5)
# L = 5
# M = 5
# N = 5

# matrix1.json (A=2x3, B=3x2)
L = 2
M = 3
N = 2

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
    dict_a = {}
    for v in list_of_values:
        if v[0] == "a":
            dict_a[v[2]] = v[3]; 
        elif v[0]=="b":
            if v[1] in dict_a:
                sum += dict_a[v[1]] * v[3]
    mr.emit((key, sum))

if __name__ == '__main__':
    inputdata = open(sys.argv[1])
    L = int(sys.argv[2])
    M = int(sys.argv[3])
    N = int(sys.argv[4])
    mr.execute(inputdata, mapper, reducer)