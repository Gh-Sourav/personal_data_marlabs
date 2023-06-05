""" Video NO 45 UDEMY"""
from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("sparkSchemaDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    flightTimeParquetDF = spark \
        .read \
        .format("parquet") \
        .load("dataSource/flight-time.parquet")

    """ This write method will give only one file and all the data should be present in that file but we
    have 2 executors so we are expecting atleast two partition but output is giving only one because we have
    not repartiton the data frame"""
    # flightTimeParquetDF.write\
    #     .format("avro")\
    #     .mode("overwrite")\
    #     .option("path", "dataSink/avro/")\
    #     .save()

    # Checking the number of partition
    logger.error("Run partition before:" + str(flightTimeParquetDF.rdd.getNumPartitions()))  # output--2 partition
    # counting the number of rows we have in each partition
    flightTimeParquetDF.groupby(spark_partition_id()).count().show()
    # In output one partition is having all the records ( 0 row - 470477)
    """ Here we will get only one file in output Instead of having two executor because We are not dividing the
    the data among two executors, That's why only one excutor is taking all the data and giving only one output 
    file"""

    partitionDf = flightTimeParquetDF.repartition(5)
    logger.error("Run partition after:" + str(partitionDf.rdd.getNumPartitions()))  # output--2 partition
    # counting the number of rows we have in each partition
    partitionDf.groupby(spark_partition_id()).count().show()

    # partitionDf.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "dataSink/avro/") \
    #     .save()
    """ Here we are writing after the data repartition so we will get 5 partition of data file as an output"""

    # Now we partition the data file using partitonBy to acheive partition elemination
    flightTimeParquetDF.write\
        .format("json")\
        .mode("overwrite")\
        .option("path", "dataSink/json/")\
        .partitionBy("OP_CARRIER", "ORIGIN")\
        .option("maxRecordsPerFile", 10000)\
        .save()
    """This "OP_CARRIER" and "ORIGIN" are the columns in the data source, We are the using them to divide the data
    # Here "OP_CARRIER" name come as the file name in the output file check datasink file
    # And "ORIGIN" becomes the subfolder inside the op_carrier folder
    # Inside that subfolder the data won't have op_carrier and origin columns
    # We are dividing the file for better optimization in spark SQL """

    """ Repartiton will divide the data equally in all partition
    partitionBy will divide the data in various size in the partition
    We can control the file size in partitionBY using 'maxRecordsPerFile' """


