def binarySearch(arr,pin):
    low=arr[0]
    high=arr[len(arr)-1]
    # mid=(low+high)/2
    for i in range(len(arr)):
        mid = (low + high) / 2
        if mid==pin:
            return mid
        elif pin>mid:
            s=arr.index(mid)
            low=arr[s+1]
        else:
            t=arr.index(mid)
            high=arr[t-1]
lst=[3,4,5,6,7,8,9]
x=4
print(binarySearch(lst,x))




