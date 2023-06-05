import time

start_time=time.time()

def binarySearch(arr,n,k):
    start_time = time.time()*1000
    low = 0
    high = n - 1
    while low <= high:
        mid = (low + high) // 2
        if arr[mid] == k:
            end_time = time.time()*1000
            timing = (start_time - end_time)
            print("Approx timing of this program for execution is ", timing)
            return mid
        elif arr[mid] > k:
            high = mid - 1
        else:
            low = mid + 1
    return -1

lst=[2,4,5,7,8,9,12,24,56,57,59,67,69,71,75,77,79,80,85,87,89]
n1=len(lst)
k1=87
res=binarySearch(lst,n1,k1)
print(res)

