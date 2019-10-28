import MapReduce
import sys

"""
Inverted Index Example
"""

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[0]
    value = record[1]
    words = value.split()
    words = list(set(words))
    for w in words:
      mr.emit_intermediate(w, key)

def reducer(key, list_of_values):
    list = []
    for v in list_of_values:
      list.append(v)
    mr.emit((key, list))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
