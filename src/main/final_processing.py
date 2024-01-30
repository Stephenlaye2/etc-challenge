from  datetime import datetime
from time import sleep
import findspark
findspark.init
from config import HDFS_PATH
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, sum
from data_schemas import *


# Initializing simple spark session
sparkSn = SparkSession.builder.master("local[*]").\
    appName("com.stephen").getOrCreate()

# Required schemas
erasure_schema = erasure_schema
product_schema = product_schema
customer_schema = customer_schema
product_transact_schema = product_transact_schema



def processStreamedData():

    today = datetime.now()
    today_date = "2020-01-01"#str(today.date())
    today_hr = str(today.time())[:2]

    # Read product data into a new dataframe
    product_df = sparkSn.read.format("json").load("{}/data-file/date={}/hour={}/product".format(HDFS_PATH, today_date, "00"))
   

    win = Window.partitionBy("customer_id").orderBy("total_cost")

  #  # Read the joined data from hdfs
    transaction_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/transaction'.format(HDFS_PATH,today_date, today_hr)).\
    withColumn("total_price", sum("total").over(win)).\
    withColumn("product_sku", col("sku")).drop("sku", "city", "postcode", "country", "address")
  
 
     # join transaction stream with product data on sku column
    product_transact = transaction_df.join(product_df, col("product_sku") == col("sku"), "inner").drop("product_sku", "price")
    
    # Stream the data to hdfs
    customer_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/customer'.format(HDFS_PATH,today_date, today_hr)).dropDuplicates(subset=["id"])

    # join customer data with transaction and product data on customer id
    combined_df = customer_df.join(product_transact, customer_df.id == product_transact.customer_id, "inner")
    
    
    final_df = combined_df.where(col("total_price") == col("total_cost")).drop("customer_id", "key")  

    # Write final dataframe to curate folder
    final_df.coalesce(1).write.mode("append").json("{}/data-file/curated-data/{}".format(HDFS_PATH, today.date().strftime("%B")))


# # The better way to do the following is to use a job scheduler to run the process every hour. Using job scheduler is more efficient
while True:
    processStreamedData()
    sleep(3600)
