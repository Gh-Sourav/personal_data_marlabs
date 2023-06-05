from pyspark.sql import SparkSession, Window
from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .master("local[2]") \
            .appName("Agg Demo")\
            .getOrCreate()

    logger = Log4j(spark)

    summery_df = spark.read.parquet("data/summary.parquet")
    summery_df.show(5)

    # Creating window function
    # We have three things to define here 1.partition 2.ordering and 3.Window start and End
    # unboundedPreceding means to take all the row from the beginning, We can give a numeric value here
    # if we give -2 in the place of Window.unboundedPreceding.
    # it'll start from 2 rows before the currentrow
    # So for -2, We'll be calculating the running total for a three-week window. try this program with -2 value
    running_total_window = Window.partitionBy("Country")\
            .orderBy("WeekNumber")\
            .rowsBetween(Window.unboundedPreceding, Window.currentRow)
    summery_df.withColumn("RunningTotal", f.sum("InvoiceValue").over(running_total_window)).show()

    # Like f.sum() we can use other aggregating function as well,
    # We can also use analytics function like denseRank, lead, lag etc