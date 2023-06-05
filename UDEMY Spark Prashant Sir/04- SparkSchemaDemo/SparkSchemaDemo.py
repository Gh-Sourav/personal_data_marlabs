from pyspark.sql import SparkSession
from pyspark.sql.types import StructType, StructField, DateType, StringType, IntegerType

from lib.logger import Log4j


# flightTimeCsvDF = spark.read \
#     .format("csv") \
#     .option("header", True) \
#     .option("inferSchema", True) \
#     .load(r"D:\Spark Udemy Prashant Sir\04- SparkSchemaDemo\data\flight-time.csv")
# Before using infer schema all the data type was in string format, but after using it some of the columns
# data type comes in a real format as int but still date column is taking a string format
# From here we can decide we can't relie on inferschema option so we have two option left explicit, implicit

def reading_csv_file(flightSchemaStruct, spark):
    # Explicit process (Programmatically)
    flightTimeCsvDF = spark.read \
        .format("csv") \
        .option("header", True) \
        .schema(flightSchemaStruct) \
        .option("mode", "FAILFAST") \
        .option("dateformat", "M/d/y") \
        .load(r"D:\Spark Udemy Prashant Sir\04- SparkSchemaDemo\data\flight-time.csv")
    # To see the exception error caused by FAILFAST after seeing the error we need way to solve the error
    # We can solve this error by using data formatting in the option()

    flightTimeCsvDF.show(5)

    # To see the data type of that data frame element
    logger.error("csv Schema:" + flightTimeCsvDF.schema.simpleString())  # The method supposed to be logger.info

    # but in my system logger.info is not working, so I used logger.error


def reading_json_file(flightSchemaDDL, spark):
    # Using DDL Schema here
    flightTimeJsonDF = spark.read \
        .format("json") \
        .schema(flightSchemaDDL) \
        .option("datetime", "M/d/y") \
        .load(r"D:\Spark Udemy Prashant Sir\04- SparkSchemaDemo\data\flight-time.json")

    flightTimeJsonDF.show(5)
    logger.error("json schema:" + flightTimeJsonDF.schema.simpleString())

    # here all int data type is showing as bigint and showing date column as string
    # It sorts the column name in alphabetical order
    # For solving these both problem we are using DDl schema and datetime format

def reading_parquet_file(spark):
    flightTimeParquetDF = spark.read \
        .format("parquet") \
        .load(r"D:\Spark Udemy Prashant Sir\04- SparkSchemaDemo\data\flight-time.parquet")
    # It provides the column data type as it is reading from data source
    # We should prefer using parquet file as long as possible because parquet file format is default and
    # recommend file format for apache Spark
    # Parquet file format will automatically take it's own schema at its best optimized format, We don't need to
    # define the schema if the schema is present in data source

    flightTimeParquetDF.show(5)
    logger.error("Parquet schema:" + flightTimeParquetDF.schema.simpleString())


if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    """ Programmatic Method..."""
    flightSchemaStruct = StructType([
        StructField("FL_DATE", DateType()),
        StructField("OP_CARRIER", StringType()),
        StructField("OP_CARRIER_FL_NUM", IntegerType()),
        StructField("ORIGIN", StringType()),
        StructField("ORIGIN_CITY_NAME", StringType()),
        StructField("DEST", StringType()),
        StructField("DEST_CITY_NAME", StringType()),
        StructField("CRS_DEP_TIME", IntegerType()),
        StructField("DEP_TIME", IntegerType()),
        StructField("WHEELS_ON", IntegerType()),
        StructField("TAXI_IN", IntegerType()),
        StructField("CRS_ARR_TIME", IntegerType()),
        StructField("ARR_TIME", IntegerType()),
        StructField("CANCELLED", IntegerType()),
        StructField("DISTANCE", IntegerType())
    ])

    """ DDL String..."""
    flightSchemaDDL = """ FL_DATE DATE, OP_CARRIER STRING, OP_CARRIER_FL_NUM INT, ORIGIN STRING, ORIGIN_CITY_NAME
     STRING, DEST STRING, DEST_CITY_NAME STRING, CRS_DEP_TIME INT, DEP_TIME INT, WHEELS_ON INT, TAXI_IN INT, 
     CRS_ARR_TIME INT, ARR_TIME INT, CANCELLED INT, DISTANCE INT"""

    reading_csv_file(flightSchemaStruct, spark)
    reading_json_file(flightSchemaDDL, spark)
    reading_parquet_file(spark)
