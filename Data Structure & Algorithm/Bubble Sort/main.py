def bubblesort(arr):
    for i in range(len(arr)):
        swapped=False
        for j in range(0,len(arr)-i-1):
            if arr[j]>arr[j+1]:
                arr[j],arr[j+1]=arr[j+1],arr[j]
                swapped=True
    if swapped==False:
        break
lst=[-2,45,0,11,-9,89,90,-100]
bubblesort(lst)
print(lst)


