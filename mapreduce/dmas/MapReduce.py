import json

class MapReduce:
    def __init__(self):
        self.intermediate = {}
        self.result = []

    def emit_intermediate(self, key, value):
        self.intermediate.setdefault(key, [])
        self.intermediate[key].append(value)

    def emit(self, value):
        self.result.append(value) 

    def execute(self, data, mapper, reducer):
        # print "MAP PHASE"
        for line in data:
            record = json.loads(line)
            mapper(record)
        
        # print "SHUFFLE PHASE"
        for key in self.intermediate:
            reducer(key, self.intermediate[key])

        # print "REDUCE"
        #jenc = json.JSONEncoder(encoding='latin-1')
        jenc = json.JSONEncoder()
        for item in self.result:
            print(jenc.encode(item))
