""" Every object is having two things, One is attributes(variables) and another one is behaviour(function)"""
class computer:
    def config(self):
        print('16gb', '1tb', 60000)


comp1 = computer()
comp2 = computer()
""" comp1 and comp2 are the object of class computer, without creating the object we can't access the class..
Like everything is a object in python, so when we are writing 
a=10 
that means we are making 'a' as an objet of integer class
Difference between comp1 and a , 'a'  is a object of in-built class Integer and comp1 is a object of computer 
class that build by ourselves"""

computer.config(comp1)
computer.config(comp2)

comp1.config()
comp2.config()
""" Line no 15 and line no 18 are doing the same thing, But we use line no 18 only because it's that whaat 
company uses and line no 15 is what happening internally when we execute line no 18"""