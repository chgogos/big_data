#!/usr/bin/env python

import sys

# input comes from STDIN (standard input)
for line in sys.stdin:
    # remove leading and trailing whitespace
    line = line.strip()
    # split the line into words
    words = line.split(';')
    # increase counters
    if len(words) != 5:
       continue # Removing error data
    
    for i in range(1,4):
      print('%d\t%d\t%s' % (i,int(words[i]), words[0]))