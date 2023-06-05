def linearSearch(arr,x):
    for i in arr:
        if i==x:
            return arr.index(i)
    return -1
lst=[3,9,2,4,6,0,1]
x=9
res=linearSearch(lst,x)
print(res)