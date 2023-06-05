from pyspark.sql import SparkSession
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[2]") \
        .appName("Group Demo") \
        .getOrCreate()

    logger = Log4j(spark)

    invoice_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("data/invoices.csv")
    # invoice_df.show(5)

    NumInvoices = f.count_distinct("InvoiceNO").alias("NumInvoices")
    TotalQuantity = f.sum("Quantity").alias("TotalQuantity")
    InvoiceValue = f.expr("round(sum(Quantity * UnitPrice),2) as InvoiceValue")

    """ Using Data Frame Expression"""
    # For calculating the week number we need to change the invoiceDate column from string to date type
    # Here we are using where for making the result data smaller though using where is not a part of the
    # transformation
    # Using weekofyear() we can find the week number of the year from any date
    exSummery_df = invoice_df.withColumn("InvoiceDate", f.to_date(f.col("InvoiceDate"), "dd-MM-yyyy H.mm")) \
        .where("year(InvoiceDate) == 2010") \
        .withColumn("WeekNumber", f.weekofyear(f.col("InvoiceDate"))) \
        .groupBy("Country", "WeekNumber") \
        .agg(NumInvoices, TotalQuantity, InvoiceValue)
    # exSummery_df.show(5)

    """Using SQL Expression"""
    # Changing the InvoiceDate column format from string to Date
    sqlSummery_df = invoice_df.withColumn("InvoiceDate", f.expr("to_date(InvoiceDate, 'dd-MM-yyyy H.mm')"))
    sqlSummery_df.createOrReplaceTempView("sales")
    exercise_sql = spark.sql("""SELECT 
                                        Country, 
                                        WEEKOFYEAR(InvoiceDate) as WeekNumber, 
                                        COUNT(DISTINCT InvoiceNo) as NumInvoices, 
                                        sum(Quantity) as TotalQuantity, 
                                        round(sum(Quantity * UnitPrice),2) as InvoiceValue
                                    FROM sales 
                                    GROUP BY country, WeekNumber""")
    # exercise_sql.show(5)

    # We want to write any(datframe exp/ SQL exp) of the transformed data frame the data into parquet file
    exSummery_df.coalesce(1)\
            .write\
            .format("parquet")\
            .mode('overwrite').save("output")
    exSummery_df.sort("Country", "WeekNumber").show()

