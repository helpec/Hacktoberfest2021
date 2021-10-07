# coding=utf-8
l = [3, 0, 2, 0, 4]

'''Simple Solution '''

'''Find left max and right max- than minimum of these two minus actual height '''


def tapp_rain_water(arr):
    w = 0
    for i in range(len(l)):
        left_max = arr[i]
        for j in range(i):
            left_max = max(left_max, arr[j])
        right_max = arr[i]
        for j in range(i + 1, len(arr)):
            right_max = max(right_max, arr[j])
        w = w + min(left_max, right_max) - arr[i]
    print (w)


tapp_rain_water(l)

'''Optimised Approach'''

''' 
Create two arrays left and right of size n. create a variable max_ = INT_MIN.
Run one loop from start to end. In each iteration update max_ as max_ = max(max_, arr[i]) and also assign left[i] = max_
Update max_ = INT_MIN.
Run another loop from end to start. In each iteration update max_ as max_ = max(max_, arr[i]) and also assign right[i] = max_
Traverse the array from start to end.
The amount of water that will be stored in this column is min(a,b) – array[i],(where a = left[i] and b = right[i]) add this value to total amount of water stored
Print the total amount of water stored.
'''


def tapp_rain_water_optimised(arr):
    water = 0
    n = len(arr)
    left_arr = [0] * n
    right_arr = [0] * n
    left_arr[0] = arr[0]
    for i in range(1, n):
        left_arr[i] = max(left_arr[i-1], arr[i])
    right_arr[n-1] = arr[n-1]
    for i in range(n-2, -1, -1):
        right_arr[i] = max(right_arr[i+1], arr[i])
    for i in range(0, n):
        water += min(left_arr[i], right_arr[i]) - arr[i]
    return water


print (tapp_rain_water_optimised(l))