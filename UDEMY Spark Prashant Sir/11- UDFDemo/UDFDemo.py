import re
from pyspark.sql.functions import udf, expr
from pyspark.sql import SparkSession
from pyspark.sql.types import StringType

from lib.logger import Log4j

""" See the gender column in survey_df.show() the gender column is containing M, Male, F, Fe, Female, Un all
these. So we want to make this column value only Male, Female, Unknown.
For that we are defining our own user define function(UDF) named as parse_gender()"""


def parse_gender(gender):
    # using regular expression to infer the male, female and unknown gender
    female_pattern = r"^f$|f.m|w.m"
    male_pattern = r"^m$|ma|m.l"
    if re.search(female_pattern, gender.lower()):
        return "Female"
    elif re.search(male_pattern, gender.lower()):
        return "Male"
    else:
        return "Unknown"


if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("UDFDemo") \
        .getOrCreate()

    logger = Log4j(spark)

    survey_df = spark.read \
        .format("csv") \
        .option("header", True) \
        .option("inferSchema", True) \
        .load("data/survey.csv")
    # survey_df.printSchema()
    survey_df.show(5)

    """ For using this UDF we need to create an Expression, However we have two approaches to develop the 
    Expression 1. Column Object Expression and 2. String Expression/ SQL Expression"""

    """ Using Column Object Expression """
    # I need to register my custom function(parse_gender()) to the Driver and make it a UDF
    # Here I am using this udf() to register my python function using the name of my local function
    # I can also specify the return type of the function, Default return type is string type
    parse_gender_udf = udf(parse_gender, StringType())

    logger.error("Catalog Entry...")
    # Seeing the function name in the catalog, Here we won't get any output cause Catalog entry is not
    # possible in the column expression
    [logger.error(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    # withColumn transformation allows us to transform a single column without impacting the other columns in
    # the data frame. It takes two argument first argument is the column I want to transform and the second
    # argument is a column Expression
    survey_df2 = survey_df.withColumn("Gender", parse_gender_udf("Gender"))
    # survey_df2.show(5)
    """ Here we are doing a 3 step process to use a user define function. first create the function
    second Register it as UDF(done it on line no 48) and get the reference(e.g parse_gender_udf)
    Now our function is register in SparkSession and Our Driver will serialize and send the function to the 
    executors, so executor can run the function smoothly
    Third Use the function in the Expression(done in line no 58)"""

    """ Using String Expression/ SQL Expression"""
    # Here the registration process is different
    # We need to register it as an SQL function and should go to the Catalog, That done using spark.udf.register()
    # First argument is the name of the UDF and the second argument is the signature of the function
    spark.udf.register("parse_gender_udf2", parse_gender, StringType())

    logger.error("Catalog Entry...")
    # Seeing the function name in the catalog, Here we'll get an output cause of one catalog entry
    [logger.error(f) for f in spark.catalog.listFunctions() if "parse_gender" in f.name]

    # Creating a equivalent SQL Expression
    survey_df3 = survey_df.withColumn("Gender", expr("parse_gender_udf2(Gender)"))
    survey_df3.show(5)
    """ Line no 48 and Line no 70, These two registration are different.
    The first one(line no 48) is to register our function as a data frame UDF, This method(udf()) will not 
    register the UDF in catalog, it will only create a UDF and serialize the function to the executors.
    The second type(line no 70) of registration is to register it as a SQL function, This one also create one 
    entry in the catalog"""
