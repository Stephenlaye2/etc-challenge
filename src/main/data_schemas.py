from pyspark.sql.types import *

erasure_schema = StructType([
    StructField("customer-id", StringType(), nullable=False),
    StructField("email", StringType(), nullable=True)
])

product_schema = StructType([
    StructField("sku", StringType(), nullable=False),
    StructField("name", StringType(), nullable=False),
    StructField("price", StringType(), nullable=False),
    StructField("category", StringType(), nullable=False),
    StructField("popularity", StringType(), nullable=False),
])

transaction_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_time", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("delivery_address", StringType(), nullable=True),
    StructField("purchases", StringType(), nullable=False)
])

address_schema = StructType([
        StructField("address", StringType(), nullable=False),
        StructField("postcode", StringType(), nullable=True),
        StructField("city", StringType(), nullable=True),
        StructField("country", StringType(), nullable=True)
        ])
purchase_schema = StructType([
        StructField("products", ArrayType(StringType()), nullable=False),
        StructField("total_cost", StringType(), nullable=True)
])
pr_schema = StructType([
            StructField("sku", StringType(), nullable=False),
            StructField("quanitity", StringType(), nullable=True),
            StructField("price", StringType(), nullable=True),
            StructField("total", StringType(), nullable=True)
        ])

customer_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("phone_number", StringType(), nullable=True),
    StructField("address", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("postcode", StringType(), nullable=True),
    StructField("last_change", StringType(), nullable=True),
    StructField("segment", StringType(), nullable=True)
])

product_transact_schema = StructType([
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_time", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("delivery_address", StringType(), nullable=True),
    StructField("purchases", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("postcode", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("total_cost", StringType(), nullable=True),
    StructField("quanitity", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("total", StringType(), nullable=True)
])

final_schema = StructType([
    StructField("id", StringType(), nullable=False),
    StructField("first_name", StringType(), nullable=False),
    StructField("last_name", StringType(), nullable=False),
    StructField("email", StringType(), nullable=False),
    StructField("phone_number", StringType(), nullable=True),
    StructField("last_change", StringType(), nullable=True),
    StructField("segment", StringType(), nullable=True),
    StructField("transaction_id", StringType(), nullable=False),
    StructField("transaction_time", StringType(), nullable=False),
    StructField("customer_id", StringType(), nullable=False),
    StructField("delivery_address", StringType(), nullable=True),
    StructField("purchases", StringType(), nullable=False),
    StructField("address", StringType(), nullable=False),
    StructField("postcode", StringType(), nullable=True),
    StructField("city", StringType(), nullable=True),
    StructField("country", StringType(), nullable=True),
    StructField("total_cost", StringType(), nullable=True),
    StructField("quanitity", StringType(), nullable=True),
    StructField("price", StringType(), nullable=True),
    StructField("total", StringType(), nullable=True)
])