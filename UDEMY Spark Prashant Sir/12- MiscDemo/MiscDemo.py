from pyspark.sql import SparkSession
from pyspark.sql.functions import *
from pyspark.sql.types import *

if __name__ == "__main__":
    spark = SparkSession.builder \
        .master("local[3]") \
        .appName("MiscDemo") \
        .getOrCreate()

    """ Quick method to create a dataframe"""
    """ How to create a data frame quickly ?? """

    data_list = [
        ("Ravi", '28', '1', '2002'),
        ("Abdul", '23', '5', '81'),
        ("john", '12', '12', '6'),
        ("Rosy", '7', '8', '63'),
        ("Abdul", '23', '5', '81')
    ]
    # This quick creation of data frame is needed for testing or exploring some techniques
    # This data frame is as good as our before created data frame
    # This toDF method(Instead Struct field, ddl string) is a shortcut to give schema to the data frame
    raw_df = spark.createDataFrame(data_list).toDF("Name", "Day", "Month", "Year").repartition(3)
    # raw_df.printSchema()

    """ How to add monotonically increasing id ?? """
    # WithColumn() used to transform an existing column and also adding column in a new data frame
    # monotonically_increasing_id() will give a unique number to every row, But the number won't be a
    # consecutive number
    df1 = raw_df.withColumn("id", monotonically_increasing_id())
    df1.printSchema()
    # df1.show()

    """ How to Use 'CASE WHEN THEN' transformation ?? """
    df2 = df1.withColumn("Year", expr("""
            CASE WHEN Year < 21 THEN Year + 2000
                WHEN Year > 21 and Year < 100 THEN Year + 1900
                ELSE Year
                END"""))
    df2.show(5)

