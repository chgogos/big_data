'''
chaining of filter - map - reduce
'''
from functools import reduce

numbers = [1, 2, 3, 4, 5, 6]
print("Initial list:", numbers)

# calculate the sum of squares for items of a list having value greater than 3
# [1, 2, 3, 4, 5, 6] -> [4, 5, 6] -> [16 + 25 + 36] -> 77
print("FilterMapReduce:", end="")
print(reduce(lambda a, b: a + b,
             map(lambda a: a**2, filter(lambda a: a > 3, numbers))))

# list comprehension as an alternative to filter map reduce
print("List comprehension:", sum([x**2 for x in numbers if x > 3]), sep="")
