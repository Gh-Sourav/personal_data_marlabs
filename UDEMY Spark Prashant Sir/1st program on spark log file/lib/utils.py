import configparser

from pyspark import SparkConf

def get_spark_app_config():
    """ This function will load the configuration from the 'spark.conf' file and return a spark_conf object"""
    spark_conf = SparkConf()
    config = configparser.ConfigParser()
    config.read("spark.conf")

    for key,val in config.items("SPARK_APP_CONFIGS"):
        spark_conf.set(key, val)
    return spark_conf

