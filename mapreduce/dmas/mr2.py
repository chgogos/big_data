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
    words = list(set(words)) # διαγραφή διπλότυπων
    for w in words:
      mr.emit_intermediate(w, key)
      # print(f"key={w} value={key}")

def reducer(key, list_of_values):
    # print(f"key={key} value={list_of_values}")
    mr.emit((key, list_of_values))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
