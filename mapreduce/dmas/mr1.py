import MapReduce
import sys

"""
Word Count Example
"""

mr = MapReduce.MapReduce()


def mapper(record):
    # key: document identifier
    # value: document contents
    key = record[0]
    value = record[1]
    words = value.split()
    for w in words:
        mr.emit_intermediate(w, 1)
        print(f"key={w} value={1}")


def reducer(key, list_of_values):
    # key: word
    # value: list of occurrence counts
    print(f"key={key} value={list_of_values}")
    total = 0
    for v in list_of_values:
        total += v
    mr.emit((key, total))


if __name__ == "__main__":
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
