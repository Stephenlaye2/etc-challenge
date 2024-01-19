from  datetime import datetime
import findspark
findspark.init
from config import HDFS_PATH
from pyspark.sql.window import Window
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import col, from_json, regexp_replace, when, hash, explode, sum
from data_schemas import *

# Initializing simple spark session
sparkSn = SparkSession.builder.master("local[*]").\
    appName("com.stephen").getOrCreate()

sparkSn.sparkContext.setLogLevel("ERROR")

# Required schemas
erasure_schema = erasure_schema
product_schema = product_schema
transaction_schema = transaction_schema
address_schema = address_schema
purchase_schema = purchase_schema
pr_schema = pr_schema
customer_schema = customer_schema
product_transact_schema = product_transact_schema
final_schema = final_schema


today_date = "2020-01-01"#str(today.date())
today_hr = "00" #str(today.time())[:2]

# Read from kafka stream
def streamer(partition):
    data_stream = sparkSn.readStream.\
    format("kafka").\
    option("kafka.bootstrap.servers", "localhost:9092").\
    option("assign", "{\"h-and-b\":[%s]}" % partition).\
    option("startingOffsets", "latest").\
    load()

    data_stream = data_stream.selectExpr("CAST(key AS STRING)", "CAST(value AS STRING)")
    return data_stream

# Processing each dataset
def stream_from_kafka():
    
    # Write erasure data set first to hdfs - dataset comes once a day
    erasure_stream = (transformData(0, "erasure", erasure_schema))
    erasure_stream.writeStream.format("json").\
        option ("path", '{}/data-file/date={}/hour=00/erasure'.format(HDFS_PATH, today_date)).\
        option("checkpointLocation", "{}/data-file/checkpoint/date={}/hour=00/erasure".format(HDFS_PATH, today_date)).\
        start()
  
  # Read the erasure dataset and get the customer id - this will be use to hash a user pid in the customer dataset
    erasure_df = sparkSn.read.option("inferSchema", True).format("json").load("{}/data-file/date={}/hour={}/erasure".format(HDFS_PATH, today_date, today_hr), schema=erasure_schema).\
        select(col("customer-id").alias("cmt_id"))
    customer_id = erasure_df.select("cmt_id").first()[0] if not erasure_df.count() == 0  else 0

    # Product data comes at the start of the day - stream product data to hdfs. sku must be unique
    product_stream = transformData(1, "product", product_schema).\
    withColumn("popularity", col("popularity").cast("float")).withColumn("price", col("price").cast("double")).\
    where(col("popularity") > 0).where(col("price") >= 0).dropDuplicates(subset=["sku"])

    product_stream.writeStream.format("json").\
        option ("path", '{}/data-file/date={}/hour={}/product'.format(HDFS_PATH, today_date, today_hr)).\
        option("checkpointLocation", "{}/data-file/checkpoint/date={}/hour={}/product".format(HDFS_PATH, today_date, today_hr)).\
        start()
    
    pquery = product_stream.writeStream.format("console").option ("truncate", False).start()

    # Read product data into a new dataframe
    product_df = sparkSn.read.option("inferSchema", True).format("json").load("{}/data-file/date={}/hour={}/product".format(HDFS_PATH, today_date, today_hr), schema=product_schema)

    # transform transaction stream
    transaction_stream = transformData(3, "transaction", transaction_schema).\
    withColumn("delivery_address", from_json("delivery_address", address_schema)).\
    withColumn("purchases", from_json("purchases", purchase_schema)).\
    select("*", "purchases.*").\
    withColumn("products", explode(col("products"))).\
    withColumn("products", from_json("products", pr_schema)).\
    select("*", "delivery_address.*", "products.*").drop("delivery_address", "purchases", "products").\
    withColumn("total_cost", col("total_cost").cast(DecimalType(30, 2))).\
    withColumn("total", col("total").cast(DecimalType(30, 2))).\
    withColumn("product_sku", col("sku")).drop("sku")
     
     # join transaction stream with product data on sku column
    product_transact = transaction_stream.join(product_df, col("product_sku") == col("sku"), "inner")

    # Stream transaction and product data to hdfs
    product_transact = product_transact.writeStream.format("json").\
        option ("path", '{}/data-file/date={}/hour={}/product_transaction'.format(HDFS_PATH,today_date, today_hr)).\
        option("checkpointLocation", "{}/data-file/checkpoint/date={}/hour={}/product_transaction".format(HDFS_PATH,today_date, today_hr)).\
        start()
    
    transtquery = transaction_stream.writeStream.format("console").option ("truncate", False).start()
   
   # Read the joined data from hdfs
    product_transact_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/product_transaction'.format(HDFS_PATH,today_date, today_hr), schema=product_transact_schema)

    # Hash customer pid (email and phone number) with id equal the id from erasure dataset
    customer_stream = transformData(2, "customer", customer_schema).\
    withColumn("email", when(col("id") == customer_id, hash("email")).otherwise(col("email"))).\
    withColumn("phone_number", when(col("id") == customer_id, hash("phone_number")).otherwise(col("phone_number"))).na.drop(subset=["id"])
    
    # join product and transaction data with customer data on customer id
    combined_df = product_transact_df.join(customer_stream, col("customer_id") == col("id"), "inner")

    customer_stream.printSchema()
    
    # Stream the data to hdfs
    transformed_df = combined_df.writeStream.format("json").\
        option ("path", '{}/data-file/date={}/hour={}/transformed-data'.format(HDFS_PATH,today_date, today_hr)).\
        option("checkpointLocation", "{}/data-file/checkpoint/date={}/hour={}/transformed-data".format(HDFS_PATH,today_date, today_hr)).\
        start()
    
    # Read the joined data, specifying the schema
    final_df = sparkSn.read.format("json").load('{}/data-file/date={}/hour={}/transformed-data'.format(HDFS_PATH,today_date, today_hr), schema=final_schema)

    # Ensuring the addition of each customer product equal to provided total cost
    win = Window.partitionBy("id").orderBy("id")
    final_df = final_df.withColumn("total_price", sum("total").over(win)).where(col("total_price") == col("total_cost")).drop("customer_id", "total_price")

    # Write final dataframe to curate folder
    final_df.write.mode("append").format("json").save("{}/data-file/date={}/hour={}/curated-data/processed.json".format(HDFS_PATH, today_date, today_hr))
    final_df.show(truncate=False)

    transformed_df.awaitTermination()
    # return query


def transformData(partition, data_key, schema):
    df = streamer(partition)
    data_stream = df.where(col("key") == data_key).select(col("key"), regexp_replace(col("value"), '^"', "").alias("value")).select(col("key"), regexp_replace( col("value"), '"$', "").alias("temp"))
    data_stream = data_stream.withColumn("temp", regexp_replace(col("temp"), '\\\\"', '"'))
    data_stream = data_stream.withColumn("temp", from_json(col("temp"), schema)).select(col("key"), col("temp.*"))

    # data_stream.writeStream.format("console").option ("truncate", False).start()

    return data_stream

    
stream_from_kafka()
print("Stream transaction data")

