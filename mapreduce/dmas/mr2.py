import MapReduce
import sys

"""
join orders and line items 
"""

mr = MapReduce.MapReduce()

def mapper(record):
    key = record[1]
    mr.emit_intermediate(key, record)

def reducer(key, list_of_values):
    order = []
    for value in list_of_values:
        if value[0]=='order':
            order=value
        else:
            mr.emit(order+value)
            
if __name__ == "__main__":
    inputdata = open(sys.argv[1])
    mr.execute(inputdata, mapper, reducer)
