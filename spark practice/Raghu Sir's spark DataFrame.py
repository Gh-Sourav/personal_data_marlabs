# importing sparkSession
from pyspark.sql import SparkSession, Row

# Starting sparkSession
sp = SparkSession.builder.appName("Basic").getOrCreate()

# Reading a json file will automatically create a Dataframe
df=sp.read.json("/spark practice/Raghu Sir's spark Dataframe's json file.json")

# It'll show all data present in a dataframe
# df.show()

# It'll show all the columns name with their data type and null value
#df.printSchema()

# It'll show only the column names
#df.columns

# Describe the whole dataframe
# df.describe()

# df.describe().show()

""" When there is no columns name specified and we need to specify by ourselves by using 'struct' data type 
That means by using Struct we can define our own schema"""
from pyspark.sql.types import StructField, StringType, IntegerType, StructType

# struct is like holding the columns
data_schema = [StructField("AGE", IntegerType(), True), StructField("NAME", StringType(), True)]
final_struc = StructType(data_schema)
df1 = sp.read.schema(final_struc).json("/spark practice/Raghu Sir's spark Dataframe's json file.json")

# df1.show()

# df1.printSchema()

# df['AGE']

# type(df['AGE'])

# sou=df.select('AGE')
#
# type(sou)

# df.head(2)

"""Creating new column..with existing data frame..But the new dataFrame can't change the original dataFrame
Because DataFrame is immutable in nature"""
# df.withColumn('newage', df['age']).show()

# Original DataFrame, DataFrame is immutable
# df.show()

# Renaming a column
# df.withColumnRenamed('age','super_new_age').show()

# some column operation
# df.withColumn('double_age', df['age']*2).show()

# df.withColumn('extra_one_year', df['age']+1).show()

"""Using SQL"""
"""To use SQL quries directly to DataFrame, You will need to register it as temporary view"""

# Registering the dataframe as SQL temporary view
# df.createOrReplaceTempView("people")
#
# sql_results = sp.sql("SELECT * FROM PEOPLE")
# sql_results.show()


""" RDD to DataFrame"""
sc = sp.sparkContext  # sparkContext is need only when we are using RDDs

# lines is a RDD here not a data frame
lines = sc.textFile('D:\spark practice/people.txt')

# lines.top(2)

# lines.take(2)

# parts is a RDD as well, and we are dividing every value on the basis of " , " operator
parts = lines.map(lambda l: l.split(" , "))

# parts.top(2) # to show the data

# Specifying the data types and making row object
people = parts.map(lambda p: Row(name=p[0], age=int(p[1])))

# people.top(2)

""" Creating Data Frame from a RDD"""

# Here people is a RDD and people_df is a dart frame
people_df = sp.createDataFrame(people)

# people_df.show()

""" Registering the data frame as a temporary table"""
# people_df.createOrReplaceTempView("people_data")
#
# teen = sp.sql(" select name from people_data")
# teen.show()


