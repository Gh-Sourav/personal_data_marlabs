""" See Full Code and output in the databricks notebook"""
from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .master("local[3]")\
            .appName('Outer join')\
            .getOrCreate()

    logger = Log4j(spark)

    orders_list = [
        ("01", "02", 350, 1),
        ("o1", "04", 580, 1),
        ("01", "07", 320, 2),
        ("02", "03", 450, 1),
        ("02", "06", 220, 1),
        ("03", "01", 195, 1),
        ("04", "09", 270, 3),
        ("04", "08", 410, 2),
        ("05", "02", 350, 1)
    ]

    orders_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

    product_list = [
        ("01", "Scroll Mouse", 250, 20),
        ("02", "Optical Mouse", 350, 20),
        ("03", "Wireless Mouse", 450, 50),
        ("04", "Wireless Keyboard", 580, 50),
        ("05", "Standard Keyboard", 360, 10),
        ("06", "16 GB Flash Storage", 240, 100),
        ("07", "32 GB Flash Storage", 320, 50),
        ("08", "64 GB Flash Storage", 430, 25)
    ]

    product_df = spark.createDataFrame(product_list).toDF("prod_id", "prod_name", "list_price", "qty")

    """ Implementing Outer join"""
    join_expr = orders_df.prod_id == product_df.prod_id

    orders_df.join(product_df, join_expr, "outer").show()


