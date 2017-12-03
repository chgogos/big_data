'''
map list of pairs (city, temperature_in_fahrenheit) to (city, temperature_in_celsius)
'''

temps =[("Athens",30),("Berlin",29),("Cairo", 36),("London",17)]
print(temps)

c_to_f = lambda city_temp: (city_temp[0], 9/5*city_temp[1]+32)
print(list(map(c_to_f, temps))) 