from pyspark.sql import SparkSession
from pyspark.sql import functions as f
from lib.logger import Log4j

if __name__ == "__main__":
    spark = SparkSession\
            .builder\
            .master("local[3]")\
            .appName("spark join demo")\
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

    order_df = spark.createDataFrame(orders_list).toDF("order_id", "prod_id", "unit_price", "qty")

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

    # order_df.show()
    # product_df.show()

    """ Implementing JOIN.."""
    # There is qty column and prod_id column present in both the data frame.
    # So while using select statement if we use only "qty" col name or only "prod_id" column name
    # Then there'll be a AMBIGUOUS_REFERENCE Exception occur
    # For solving that problem there is two-way
    # 1. Rename the specific/ambiguous column and use the new name
    product_renamed_df = product_df.withColumnRenamed("qty", "reorder_qty")
    # 2. Drop the one column after .join() and before .select()
    join_expr = order_df.prod_id == product_df.prod_id
    order_df.join(product_renamed_df, join_expr, "inner")\
            .drop(product_renamed_df.prod_id)\
            .select("order_id", "prod_id", "prod_name", "unit_price", "qty")\
            .printSchema()

    """ When we select all the columns( select(*) ) then this ambiguity didn't happened. But when we explicitly
    specifying the column name(e.g .select("order_id", "prod_id", "prod_name", "unit_price", "qty") ) there'll
    be a ambiguity exception
    Why Spark is doing this??
    - Every data frame column has a unique id in the catalog and the spark engine always work using those internal 
    ids. These internal ids are not shown to us and we are expected to work with the column names. However this 
    spark engine will translate the column name to ids during the analysis phase
    
    - When I refer the column name in the expression then this spark engine will internally translate the column
    name to column id and that's where it's complain about ambiguity.
    
    - But when I am using select(*) it takes all the column ids and shows them, Translation from column name 
    from column id is not needed """




