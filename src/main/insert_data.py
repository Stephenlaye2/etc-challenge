

from datetime import datetime
from time import sleep
from pydoop import hdfs
import gzip
import kafka_producer
import config

# Check for file arrival and send data to kafka topic
class FetchAndProduce:
    def __init__(self, hdfs_path, p_key, partition):
        self.hdfs_path = hdfs_path
        self.processed_path = ''
        self.topic_name = "h-and-b"
        self.partition = partition
        self.p_key = p_key


# Check if a specific file exists
    def is_file_present(self):
        is_exists = hdfs.path.exists(self.hdfs_path)
        return is_exists
# Check file arrival. send file content to topic iteratively. Avoid processing previously processed file
    def checkArrival(self):
        if self.is_file_present() == True:
            if self.hdfs_path != self.processed_path:
               for json_data in self.fetch_data():
                    self.produce(json_data.strip('"'))
                    sleep(1)
               self.processed_path = self.hdfs_path

# Unzip gzip file and extract file data. Decode to utf-8 and convert to list
    def fetch_data(self):

        with hdfs.open(self.hdfs_path) as gzip_file:
            with gzip.GzipFile(mode='rb',fileobj=gzip_file) as f:
                
                text_data = f.read()
                return text_data.decode("utf-8").split("\n")
            
# send to topic at a particular partition with a unique key
    def produce(self, data):

        return kafka_producer.send_data(topic_name=self.topic_name, p_key=self.p_key, data=data, partition_num=self.partition)

today = datetime.now()
today_date = "2020-01-01"#str(today.date())
today_hr = "00" #str(today.time())[:2]
hdfs_path = config.HDFS_PATH


erasure_hdfs_path = "{}/date={}/hour={}/erasure.json.gz/".format(hdfs_path, today_date, today_hr)
product_hdfs_path = "{}/date={}/hour={}/products.json.gz/".format(hdfs_path, today_date, today_hr)
customer_hdfs_path = "{}/date={}/hour={}/customers.json.gz/".format(hdfs_path, today_date, today_hr)
transact_hdfs_path = "{}/date={}/hour={}/transactions.json.gz/".format(hdfs_path, today_date, today_hr)



erasure = FetchAndProduce(erasure_hdfs_path, "erasure", 0)
product = FetchAndProduce(product_hdfs_path, "product", 1)
customer = FetchAndProduce(customer_hdfs_path, "customer", 2)
transaction = FetchAndProduce(transact_hdfs_path, "transaction", 3)


# Poking file directory for activities
while True:
    erasure.checkArrival()
    product.checkArrival()
    transaction.checkArrival()
    customer.checkArrival()


