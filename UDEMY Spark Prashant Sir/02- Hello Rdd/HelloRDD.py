import sys
from collections import namedtuple

from pyspark import SparkConf, SparkContext
from pyspark.sql import SparkSession
from lib.logger import Log4j

if __name__ == "__main__":
    # conf = SparkConf \
    #     .setMaster(value="local[3]") \
    #     .setAppName("HelloRdd")
    # sc = SparkContext(conf = conf)

    """ Here we will get spark context from spark session beacause spark session is built on spark context for 
    better optimization and ease of writing programming languages"""
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloRDD") \
        .getOrCreate()

    sc = spark.sparkContext  # we are getting spark context from spark session
    logger = Log4j(spark)  # setting up log file

    # Creating header for RDD using set
    surveyRecord = namedtuple("SurveyRecord", ["Age", "Gender", "Country", "State"])
    # Finding the file name and location dynamically
    if len(sys.argv) != 2:
        logger.error("Usage: HelloRdd <filename>")
        logger.error((sys.argv))
        sys.exit(-1)

    # Reading the text file as an RDD directly
    linesRDD = sc.textFile(sys.argv[1])
    partitionRDD = linesRDD.repartition(2)

    colsRDD = partitionRDD.map(lambda line: line.replace(' " ', '').split(','))
    selectRDD = colsRDD.map(lambda cols: surveyRecord(int(cols[1]), cols[2], cols[3], cols[3]))
    filterRDD = selectRDD.map(lambda r: r.Age < 40)
    kvRDD = filterRDD.map(lambda r: (r.Country, 1))
    countRDD = kvRDD.reduceByKey(lambda v1, v2: v1 + v2)

    colsList = colsRDD.collect()
    for x in colsList:
        logger.info(x)
