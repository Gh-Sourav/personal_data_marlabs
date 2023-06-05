

# ls=[9, 8, 7, 6, 4, 2, 1, 3]
# # ele1 = ls.pop(0)
# ele2 = ls.pop(7)
# print( ele2)
# print(ls)
# ls.insert(0,ele2)
# # ls.append(ele1)
# print(ls)


# s = 'coOL dog'   # cOOl dOg
# s1 = ''
# for i in s:
#     if i.isspace():
#         break
#     else:
#         s1 += s.casefold()
# print(s1)

# n=4
# if n in range(2,6):
#     print("Not Weird")

# res=[]
# lst1=[]
# lst=["H1","H2","H3","H2","H3","H1"]
# for i in lst:
#     if i not in res:
#         res.append(i)
# final=res.copy()
# lst1=res+final
# lst1.append(lst[0])
# print(lst1)

# lst=[2,4,6,8]
# func=list (map(lambda x: x * 2, lst))
# print(func)

# fun=lambda x,y:x*y
# print(fun(200,4))

# num=int(input("Enter the number you want to multiply with:"))
# fun=lambda x : x * num
# print(fun(15))

# def function_compute(n):
#     return lambda x: x*n
# result = function_compute(2)
# print("Double the number of 15:", result(15))
# result=function_compute(3)
# print("Triple the number of 15:", result(15))
# result=function_compute(4)
# print("Quadruple the number of 15:", result(15))

# subject_marks=[('English', 88), ('Science', 90), ('Maths', 97), ('Social sciences', 82)]
# print("Original list:",subject_marks)
# subject_marks.sort(key=lambda x:x[1])
# print("sorted list:",subject_marks)

# example_gks=[('for', 24), ('Geeks', 8), ('Geeks', 30)]
# print("original list:",example_gks)
# example_gks.sort(key= lambda x:x[1])
# print("Sorted list:",example_gks)

# example_gks=[('for', 24), ('is', 10), ('Geeks', 28),('Geeksforgeeks', 5), ('portal', 20), ('a', 15)]
# num=len(example_gks)
# for i in range(0,num):
#     for j in range(0,num-i-1):
#         if example_gks[j][1]>example_gks[j+1][1]:
#             # example_gks[j],example_gks[j+1]=example_gks[j+1],example_gks[j]
#             temp=example_gks[j]
#             example_gks[j]=example_gks[j+1]
#             example_gks[j+1]=temp
# print(example_gks)

# lst=[{'make': 'Nokia', 'model': 216, 'color': 'Black'}, {'make': 'Mi Max', 'model': 2, 'color': 'Gold'}, {'make': 'Samsung', 'model': 7, 'color': 'Blue'}]
# print("original list",lst)
# print(models)
# lst.sort(key= lambda x:x['model'])
# print("sorted list",lst[::-1])

# lst=[{'make': 'Nokia', 'model': 216, 'color': 'Black'}, {'make': 'Mi Max', 'model': 2, 'color': 'Gold'}, {'make': 'Samsung', 'model': 7, 'color': 'Blue'}]
# print("Original list",lst)
# len_of_lst=len(lst)
# for i in range(0,len_of_lst):
#     for j in range(0,len_of_lst-i-1):
#         if lst[j]['model']<lst[j+1]['model']:
#             lst[j],lst[j+1]=lst[j+1],lst[j]
# print("sorted list:",lst)

# lst=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# lsto=list(filter(lambda x:x%2!=0,lst))
# lste=list(filter(lambda x:x%2==0,lst))
# print("Even number is:",lste)
# print("Odd number is:",lsto)

# lst=[1, 2, 3, 4, 5, 6, 7, 8, 9, 10]
# squre_lst=list(map(lambda x:x**2,lst))
# Qube_lst=list(map(lambda x:x**3,lst))
# print("Squre of the list:",squre_lst)
# print("Qube of the list:",Qube_lst)

# String=input("Write your name:")
# char=input("Write any charcter of your choice:")
# # if String[0]==char:
# #     print(True)
# # else:
# #     print(False)
# sam=lambda x,y:x[0]==y
# print(sam(String,char))

# starts_with=lambda x: True if x.startswith("z") else False
# print(starts_with("zebra"))

# import datetime
# now=datetime.datetime.now()
# print(now)
# year=lambda x:x.year
# month=lambda x:x.month
# day=lambda x:x.day
# tim=lambda x:x.time()
# print(year(now))
# print(month(now))
# print(day(now))
# print(tim(now))

# str=input("Enter a no or any string:")
# is_num = lambda x: x.replace(".","",1).isdigit()
# is_num1= lambda r: is_num(r[1:]) if r[0]=="-" else is_num(r)
# print(is_num1(str))

# no_to_go=int(input("Enter the no of element needed in a fibonaci series:"))
# a=0
# b=1
# print(a,b)
# for i in range(2,no_to_go):
#     c = a + b
#     a=b
#     b=c
#     print(c)

# import os
# import xml.etree.ElementTree as ET
# path= "D:\Data Structure & Algorithm"
# dir_list= os.listdir(path)
# for i in dir_list:
#     if i.endswith(".py"):
#         print(i)
#         # fullname = os.path.join(path, i)
#         # tree = ET.parse(fullname)
#         # print(tree)

import glob
import os
from pyspark.sql import SparkSession

location_lst=[]
sp = SparkSession.builder.appName("Basic").getOrCreate()
path= "D:\Data Structure & Algorithm"
for filename in glob.glob(os.path.join(path,'*','*.json')):
    location_lst.append(filename)
one_df = sp.read.json(location_lst[0])
one_df.show()

















