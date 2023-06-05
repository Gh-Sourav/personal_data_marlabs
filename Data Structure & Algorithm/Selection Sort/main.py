def selectionSort(arr):
    min=arr[0]
    for i in range(len(arr)):
        for j in range(0,len(arr)-i-1):
            if arr[j]>arr[j+1]:
                min=arr[j+1]
        arr[i],min=min,arr[i]
lst=[20,12,10,15,2]
print(selectionSort(lst))