"""" Video No 47 Udemy"""
from pyspark.sql import SparkSession

from lib.logger import Log4j

if __name__ == '__main__':
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("SparkSQLTableDemo") \
        .enableHiveSupport() \
        .getOrCreate()
    # We need a persistence/permanent HIVE meta store to store the metadata for managed table, That's why
    # we are use enableHiveSupport()

    logger = Log4j(spark)

    flightTimeParquetDF = spark \
        .read \
        .format("parquet") \
        .load("dataSource/")

    """ We are writing/saving the data as table format in spark managed tables"""

    # Spark is having a default database names "default_db", If we don't mention the database name while writing
    # the table name or before, then spark by default store the table in this default database
    # Here we want to store our flight_data_tbl in our own database named "AIRLINE_DB"
    # first way is hardcode -
    # flightTimeParquetDF.write.mode("overwrite").saveAsTable("AIRLINE_DB.flight_data_tbl")
    # Second way is -

    # creating the database to make sure it exits
    spark.sql("CREATE DATABASE IF NOT EXISTS AIRLINE_DB")

    # Access the catalog and set the current database
    spark.catalog.setCurrentCatalog("AIRLINE_DB")

    # flightTimeParquetDF.write\
    #     .mode("overwrite")\
    #     .partitionBy("ORIGIN", "OP_CARRIER")\
    #     .saveAsTable("flight_data_tbl")

    flightTimeParquetDF.write \
        .mode("overwrite") \
        .bucketBy(5, "OP_CARRIER", "ORIGIN") \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_data_tbl")

    # We should use logger.info instead of logger.error, here in this machine logger.info is not working
    logger.error(spark.catalog.listTables("AIRLINE_DB"))