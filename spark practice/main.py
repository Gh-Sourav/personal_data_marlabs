import findspark
# from pyspark import sparkcontext
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, col, when
from pyspark.sql.functions import avg
from pyspark.sql.functions import lit
findspark.init()

#initializing spark
sp=SparkSession.builder.appName("owning").getOrCreate()

data = [
    ("Sourav", 67, 21),
    ("Sourav1", 47, 21),
    ("Sourav2", 57, 21),
    ("Sourav3", 77, 21),
    ("Sourav4", 77, 21),
    ("Sourav5", 87, 21),
    ("Sourav6", 97, 21)
]

header = ["NAme", "MArks", "Age"]

#creating dataframe
df = sp.createDataFrame(data,header)
# df.show()
# df.select(avg("Age")).show()

# Printing table schema
# df.printSchema()

# Printing only selected columns
# df.select("name","marks").show(5)

# withColumn function is used to manipulate a column or to create a new column with the existing columns.
# df.withColumn("RollNo", df.MArks-40).show()

# Same as groupby function in sql
# df.groupby("Age").count().show()

# Split function to split any column
# For using split function we need to import it first
# df1=df.withColumn("Name1", split(df["NAme"],"v").getItem(0)) .withColumn("No",split(df["NAme"],"v").getItem(1))
# df1.select("NAme","Name1","No").show()

# lit function is used to add a column in dataframe with literals or some constant value
# df2=df.select(col("NAme"),lit(80).alias("Quality of all"))
# df2.show()

# df.show()
# df.select("name",when(df.MArks>=77),"57").show()

# Write in a csv file
# df.write.format("csv").save("mydetails")

# SELECT statement uses
# newdf=df.select("Name","Age")
# newdf.show()
locationdf=df.select("Name",col("Name").alias("UserName"))
locationdf.show()