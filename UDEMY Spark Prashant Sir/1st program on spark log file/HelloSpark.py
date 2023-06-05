from pyspark.sql import *
from lib.logger import log4j
from lib.utils import get_spark_app_config

if __name__ == "__main__":
    conf = get_spark_app_config()
    spark = SparkSession.builder.config(conf=conf).getOrCreate()

    logger = log4j(spark)

    logger.info("Starting Hello spark")
    # My processing code
    conf_out = spark.sparkContext.getConf()
    logger.info(conf_out.toDebugString())

    logger.info("Finishing Hello spark")
    # spark.stop()
