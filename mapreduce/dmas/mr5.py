import MapReduce
import sys

"""
DNA
"""

mr = MapReduce.MapReduce()

def mapper(record):
  sequence = record[1][:-10]
  mr.emit_intermediate(sequence, 1)

def reducer(key, list_of_values):
  mr.emit(key)

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
