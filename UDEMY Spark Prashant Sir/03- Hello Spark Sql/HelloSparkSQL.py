from pyspark.sql import SparkSession
from tkinter import filedialog as fd
from lib.logger import Log4j

if __name__ == "__main__":
    """ Configuring the sparksession"""
    spark = SparkSession \
        .builder \
        .master("local[3]") \
        .appName("HelloSparkSql") \
        .getOrCreate()
    # Configuring the log file
    logger = Log4j(spark)

    # Using tkinter to find out the data file name and location
    file_name = fd.askopenfilename()
    # print(file_name)

    # Reading the csv file
    surveyDf = spark.read \
        .option("Header", True) \
        .option("inferSchema", True) \
        .csv(file_name)
    # surveyDf.show(10)

    # Registering a dataframe as view to run sql queries on it
    surveyDf.createOrReplaceTempView("survey_tab")
    count_df = spark.sql("Select country, count(*) as count from survey_tab where age <40 group by country")
    count_df.show(10)
