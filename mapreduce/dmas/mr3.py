import MapReduce
import sys

mr = MapReduce.MapReduce()


def mapper(record):
    key = record[1]
    mr.emit_intermediate(key, record)


def reducer(key, list_of_values):
    row = []
    for record in list_of_values:
        if record[0] == 'order':
            row = record
        else:
            mr.emit(row+record)


if __name__ == "__main__":
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
