'''
compute areas for a set of circles with know radii
example with map and lambda
'''
import math

def area(radius):
    '''
    area of a circle
    '''
    return math.pi * (radius**2)

RADIUSES = [2, 5, 7.1, 0.3, 10]

# method 1
AREAS = []
for x in RADIUSES:
    a = area(x)
    AREAS.append(a)

print(AREAS)

# method 2 using map
print(list(map(area, RADIUSES)))

# method 3 using map and lambda
print(list(map(lambda x: math.pi * (x**2), RADIUSES)))
