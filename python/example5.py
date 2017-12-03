'''
chaining of map - filter - reduce
list comprehensions as an alternative to map filter reduce
'''
from functools import reduce

numbers = [1, 2, 3, 4, 5, 6]
print("Initial list:", numbers)

# calculate the sum of squares for items of a list having value greater than 3
# [1, 2, 3, 4, 5, 6] -> [4, 5, 6] -> [16 + 25 + 36] -> 77
print("FilterMapReduce:", end="")
print(reduce(lambda a, b: a + b,
             map(lambda a: a**2, filter(lambda a: a > 3, numbers))))
print("List comprehension:", sum([x**2 for x in numbers if x > 3]), sep="")

# calculate squares of each item in the list, filter those that are greater than 3, then sum
# [1, 2, 3, 4, 5, 6] -> [1, 4, 9, 16, 25, 36] -> [4 + 9 + 16 + 25 + 36] -> 90
print("MapFilterReduce:", end="")
print(reduce(lambda a, b: a + b, filter(lambda a: a > 3, map(lambda a: a**2, numbers))))
print("List comprehension:", sum([x for x in [y**2 for y in numbers] if x > 3]), sep="")
