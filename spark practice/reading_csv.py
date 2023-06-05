import sqlite3
import findspark
findspark.init()
from pyspark.sql import SparkSession

conn=sqlite3.connect("Nutration_Database.db")
cur=conn.cursor()

ss=SparkSession.builder.appName("Working_with_csv").getOrCreate()
main_path="/spark practice/cereal.csv"

userdf=ss.read.option("sep","\t").option("header",True).option("inferSchema",True).csv(main_path)
# userdf.show()

userdf.write.option("compression","snappy").mode("overwrite").csv("./Nutration_Database.db")
conn.commit()
conn.close()


