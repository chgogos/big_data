import MapReduce
import sys

"""
reveal asymmetric friendship relationships
"""

mr = MapReduce.MapReduce()

def mapper(record):
    person = record[0]
    friend = record[1]
    mr.emit_intermediate(person, record)
    mr.emit_intermediate(friend, record)

def reducer(key, list_of_values):
    # print "key=%s value=%s" % (key, list_of_values)
    for v in list_of_values:
        if [v[1],v[0]] not in list_of_values:
          if (v[0] == key):
            mr.emit("For %s relationship with %s is asymmetric" % (v[0],v[1]))
          else:
            mr.emit("For %s relationship with %s is asymmetric" % (v[1],v[0]))

if __name__ == '__main__':
  inputdata = open(sys.argv[1])
  mr.execute(inputdata, mapper, reducer)
