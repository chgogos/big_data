from functools import reduce

data = [4,2,6,5,2,8]

res1=1
for x in data:
    res1 *= x
print(res1)

res2 = reduce(lambda x,y:x*y, data)
print(res2)