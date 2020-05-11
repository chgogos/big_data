#!/usr/bin/env python

import sys

def getKey(item):
  return item[0]

def print_res(max_nums, NF):
  str_list = sorted(max_nums, key=getKey, reverse=True)

  for i in range(0,min(len(str_list),NF)):
    print('%d\t%d\t%s' % (current_map_key, str_list[i][0], str_list[i][1]))


current_map_key = None 
word = None
NF=10

max_nums = []

# input comes from STDIN
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()

    # parse the input we got from mapper.py
    mapper_key, count, word = line.split('\t', 3)

    # convert count (currently a string) to int
    try:
        count = int(count)
        mapper_key = int(mapper_key)
    except ValueError:
        # count was not a number, so silently
        # ignore/discard this line
        continue

    if current_map_key is None:
      current_map_key = mapper_key
    elif current_map_key != mapper_key:
      #we received key that is different than the initial, so ignore
      print_res(max_nums, NF)
      print('Change')
      max_nums = []
      current_map_key = mapper_key
      

    max_nums.append((count,word))

print_res(max_nums, NF)