from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName('Agg Demo') \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("data/invoices.csv")
    # invoice_df.show(10)
    # invoice_df.printSchema()

    """ SIMPLE AGGREGATION...."""
    # Simple Aggregation always give one line output

    """ Column Object Expressions"""
    colSelect_df = invoice_df.select(f.count("*").alias("CountAll"),
                                     f.sum("Quantity").alias("TotalQuantity"),
                                     f.avg("UnitPrice").alias("AvgPrice"),
                                     f.count_distinct("InvoiceNo").alias("CountDistinct"))
    # colSelect_df.show()

    """ SQL Expression/ String Expression"""
    SqlSelect_df = invoice_df.selectExpr("count(1) as count1 ",
                                         "count(StockCode) as countfield ",
                                         "sum(Quantity) as TotalQuantity ",
                                         "avg(UnitPrice) as AvgPrice "
                                         )
    # SqlSelect_df.show()

    # count(1) and count(*) are same, it'll count all the rows irrespective of the value, if the value is NULL
    # in the row then also it'll count that as row as 1 row
    # if we use any field/column count(e.g count(StockCode) ) then it'll ignore any null value in that
    # column/field

    """ GROUPING AGGREGATION...."""
    # Multiple line output

    """ Using SQL Expression"""
    invoice_df.createOrReplaceTempView("sales")
    summery_sql = \
        spark.sql("""SELECT 
                        country, 
                        InvoiceNo, 
                        sum(Quantity) as TotalQuantity, 
                        ROUND(SUM(Quantity * UnitPrice)) as InvoiceValue 
                    FROM sales 
                    GROUP BY country, InvoiceNo"""
                  )
    # summery_sql.show()

    """ Using Data Frame Expression"""
    summery_df = invoice_df \
        .groupby('Country', 'InvoiceNo') \
        .agg(f.sum('Quantity').alias('TotalQuantity'),
             f.round(f.sum(f.expr("Quantity * UnitPrice")), 2).alias('InvoiceValue'),
             f.expr("ROUND(SUM(Quantity * UnitPrice)) as InvoiceValue ")
             )
    # summery_df.show()
