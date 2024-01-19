from  time import sleep
import findspark
findspark.init
from config import HDFS_PATH
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum
from data_schemas import *

# Initializing simple spark session
sparkSn = SparkSession.builder.master("local[2]").\
    appName("com.stephen").getOrCreate()

# Required schemas
erasure_schema = erasure_schema
product_schema = product_schema

customer_schema = customer_schema
product_transact_schema = product_transact_schema


today_date = "2020-01-01"#str(today.date())
today_hr = "00" #str(today.time())[:2]

def processStreamedData():

    # Read product data into a new dataframe
    product_df = sparkSn.read.option("inferSchema", True).format("json").load("{}/data-file/date={}/hour={}/product".format(HDFS_PATH, today_date, today_hr), schema=product_schema)

   # Read the joined data from hdfs
    transaction_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/transaction'.format(HDFS_PATH,today_date, today_hr), schema=product_transact_schema).\
    withColumn("product_sku", col("sku")).drop("sku", "city", "postcode", "country", "address")

    transaction_df.show(truncate=False)
     # join transaction stream with product data on sku column
    product_transact = transaction_df.join(product_df, col("product_sku") == col("sku"), "inner").drop("product_sku", "price")

    # Stream the data to hdfs
    customer_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/customer'.format(HDFS_PATH,today_date, today_hr), schema=customer_schema)

      # join product and transaction data with customer data on customer id
    combined_df = product_transact.join(customer_df, col("customer_id") == col("id"), "inner")


    # Ensuring the addition of each customer product equal to provided total cost
    win = Window.partitionBy("id").orderBy("id")
    final_df = combined_df.withColumn("total_price", sum("total").over(win)).where(col("total_price") == col("total_cost")).drop("customer_id", "total_price")

    # Write final dataframe to curate folder
    
    if final_df.count() != 0:
            final_df.write.mode("append").format("json").json("{}/data-file/date={}/hour={}/curated-data/processed.json".format(HDFS_PATH, today_date, today_hr))
            final_df.show(truncate=False)


# The better way to do the following is to use a job scheduler to run the process every hour. Using job scheduler is more efficient
while True:
    processStreamedData()
    sleep(3600)