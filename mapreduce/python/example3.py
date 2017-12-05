'''
filter and keep values greater than the average
'''
import statistics

data = [1.3, 2.7, 3.4, 5.2, 3.2, 7.1]
print(data)

avg = statistics.mean(data)
print(avg)

it = filter(lambda x: x>avg, data)
print(list(it))
