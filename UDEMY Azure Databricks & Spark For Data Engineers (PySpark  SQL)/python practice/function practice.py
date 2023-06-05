str = 'thEre Is Always a sound in siLe$nce'
no = '50'

# print(str.capitalize())

# print(str.casefold())

# print(str.center(no))

# print(str.count("a"))

# print(str.encode())

# print(str.endswith('siLe$nce'))

# print(str.expandtabs(70))

# print(str.find('I'))

# mt= 'my name is {fname}, my age is {age}'.format(fname='Sourav',age='24')
# print(mt)

# print(str.index('I'))

# print(str.isalnum())# give false for blank space and special characters as well

# print(str.isalpha()) # give true if there is only alphabet persent in the string

# print(no.isnumeric())

# print(str.isprintable())

# print(str.istitle())

lst = ['6', '9', '-3', '-8', '0', '45', '-98', '65', '34', '10', '5', '6', '9', '-8', '-67', '-90', '-19', '0']
str12 = ",".join(lst)
# print(str12)

# x='Apple'
# nothing=x.ljust(20)
# print(nothing + ' is my favourite fruit')

# print(str.lower())

# print(str.lstrip(''))

# mytable=str.maketrans('I','S') # It'll change the position of I to S
# print(str.translate(mytable))

# part=str.partition("Is") # It'll partition the string in three parts and returns a tuple
# print(part)

# rep=str.replace("s", "App")
# print(rep)

# print(str.rfind("I"))
# print(str.find("I"))

# print(str.split("s"))
# print(str.rsplit("s"))

# print(str.swapcase())

# print(str.splitlines())

# print(str.title())

# print(str.translate("I"))

# print(str.zfill(3))

import requests
from bs4 import BeautifulSoup

res = requests.get('https://www.tutorialspoint.com/tutorialslibrary.html')
print("The status code is", res.status_code)
print("\n")
soup_data = BeautifulSoup(res.text, 'html.parser')
print(soup_data.title)
print("\n")
