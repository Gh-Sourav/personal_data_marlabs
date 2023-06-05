from pyspark.sql import *
from pyspark import SparkContext
import pandas as pd
from tkinter import  filedialog as fd

spark = SparkSession.builder.master("local[3]").appName("Attendence_Manupulation").getOrCreate()

# Using tkinter to get the data file location
filename = fd.askopenfilename()

# Reading the excel file into pandas dataframe
# Using sheet name because there is more than one sheet in that file name
column_names = ["Employee Name ", "Jan", "Feb", "Mar", "Grand Total"]
pan_df = pd.read_excel(filename, sheet_name="Pivot 1")
# print(pan_df)

# Delete Rows by Index Labels
clean_pan_df = pan_df.drop(index=[0, 1])

spark_df = spark.createDataFrame(pan_df)
spark_df.show()
#
# # print(sqldf("select * from clean_pan_df limit 10", globals()))
#
# # print(sqldf("select employee name, grand total  from clean_pan_df where grand total = (select max(grand total) from clean_pan_df"))
#
# print(sqldf("select * from clean_pan_df order by grand total", globals()))
