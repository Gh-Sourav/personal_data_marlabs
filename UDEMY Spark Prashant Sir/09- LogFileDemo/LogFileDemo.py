""" Here the data is unstructured, So before transforming the data we need to give a structure to the data"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import *

if __name__ == "__main__":
    spark = SparkSession\
        .builder\
        .master("local[3]")\
        .appName("LogFileDemo")\
        .getOrCreate()

    # Here we are seeing the data before giving the structure
    # Here all thr data will be printing as one column
    file_df = spark.read.text("data/apache_logs.txt")
    file_df.printSchema()
    file_df.show()

    # The data is having total 11 columns such as 1.IP 2.Client 3.user 4.datetime 5.cmd 6.request 7.protocol
    # 8.status 9.bytes 10.referrer 11.userAgent, We'll be giving a schema in the data using regex
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4}\] "(\S+) (\S+) (\S+)" (\d{3}) (\s+) "(\s+)" "([^"]*)'

    # regexp_extract function is an inbuilt function in pyspark.sql.functions
    # "value" is the column name for file_df data frame after reading, we can see it from line no 15
    # This log_reg'll select all the 11 columns so we are specifying only 4 column we need in this example
    logs_df = file_df.select(regexp_extract("value", log_reg, 1).alias("Ip"),
                             regexp_extract("value", log_reg, 4).alias("date"),
                             regexp_extract("value", log_reg, 6).alias("request"),
                             regexp_extract("value", log_reg, 10).alias("referrer"))
    logs_df.printSchema()

    logs_df.withColumn("referrer", substring_index("referrer", "/", 3))\
        .groupby("referrer")\
        .count()\
        .show(100, truncate=False)



