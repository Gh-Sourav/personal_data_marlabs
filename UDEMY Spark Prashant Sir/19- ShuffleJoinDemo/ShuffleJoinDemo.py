from pyspark.sql import SparkSession
# from pyspark.sql import functions as f

from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .master("local[3]")\
            .appName("Shuffle Join Demo")\
            .getOrCreate()

    logger = Log4j(spark)

    # Each data frame will be having 3 partition by default because they are
    flight_time_df1 = spark.read.json("data/d1/")
    flight_time_df2 = spark.read.json("data/d2/")

    # flight_time_df1.show(5)
    # flight_time_df2.show(5)

    # This config will ensure that we get 3 partition after the shuffle which means 3 reduce exchanges
    spark.conf.set("spark.sql.shuffle.partitions", 3)

    join_expr = flight_time_df1.id == flight_time_df2.id
    join_df = flight_time_df1.join(flight_time_df2, join_expr, "inner")

    # Just holding the spark execution to see the UI
    join_df.collect()
    input("press a key to stop..")

