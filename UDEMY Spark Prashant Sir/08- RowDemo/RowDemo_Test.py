from datetime import date
from unittest import TestCase
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from RowDemo import to_date_df


class RowDemoTestCase(TestCase):

    @classmethod
    def setUpClass(cls) -> None:
        cls.spark = SparkSession \
            .builder \
            .master("local[3]") \
            .appName("RowDemoTest") \
            .getOrCreate()

        my_schema = StructType([
            StructField("Id", StringType()),
            StructField("EventDate", StringType())
        ])

        my_rows = [Row["123", "04/05/2020"], Row["124", "4/05/2020"], Row["125", "04/05/2020"], Row["126", "4/05/2020"]]
        my_rdd = cls.spark.sparkContext.parallelize(my_rows, 2)
        cls.my_df = cls.spark.createDataFrame(my_rdd, my_schema)

        def test_data_type(self):
            """ Here we are going to validate the data type"""
            rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()  # This data frame present in the executor
            # for validating the data type we need to collect the data frame from executor to driver
            for row in rows:
                self.assertIsInstance(row["EventDate"], date)  # Validating this as an instance of date

        def test_data_value(self):
            """ Here we are validating the data itself"""
            rows = to_date_df(self.my_df, "M/d/y", "EventDate").collect()
            for row in rows:
                self.assertEqual(row["EventDate"], date(2020, 4, 5))  # we are asserting the date value here
