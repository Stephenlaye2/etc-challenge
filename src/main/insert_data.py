
from datetime import datetime
from time import sleep
from pydoop import hdfs
import gzip
import kafka_producer
import config

# Check for file arrival and send data to kafka topic
class FetchAndProduce:
    def __init__(self, hdfs_path="", p_key="", partition=None):
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
            for json_data in self.fetch_data():
                self.produce(json_data.strip('"'))
                # sleep(1)

# Unzip gzip file and extract file data. Decode to utf-8 and convert to list
    def fetch_data(self):

        with hdfs.open(self.hdfs_path) as gzip_file:
            with gzip.GzipFile(mode='rb',fileobj=gzip_file) as f:
                
                text_data = f.read()
                return text_data.decode("utf-8").split("\n")
            
# send to topic at a particular partition with a unique key
    def produce(self, data):

        return kafka_producer.send_data(topic_name=self.topic_name, p_key=self.p_key, data=data, partition_num=self.partition)


def getDateTime():
    return (datetime.now().date(), str(datetime.now().time())[:2])

today_date = "2020-01-01" #getDateTime()[0]
today_hr = getDateTime()[1]
hdfs_path = config.HDFS_PATH

prev_erasure_path = ''
prev_product_path = ''
prev_customer_path = ''
prev_transaction_path = ''


def produceData( hdfs_path, key, partition):
        fetch_and_pr = FetchAndProduce(hdfs_path, key, partition)
        fetch_and_pr.checkArrival()
        print(hdfs_path)
    


# Poking file directory for activities
while True:
    erasure_hdfs_path = "{}/date={}/hour={}/erasure.json.gz/".format(hdfs_path, today_date, today_hr)
    product_hdfs_path = "{}/date={}/hour={}/products.json.gz/".format(hdfs_path, today_date, today_hr)
    customer_hdfs_path = "{}/date={}/hour={}/customers.json.gz/".format(hdfs_path, today_date, today_hr)
    transact_hdfs_path = "{}/date={}/hour={}/transactions.json.gz/".format(hdfs_path, today_date, today_hr)
  
    if prev_erasure_path != erasure_hdfs_path:
        produceData(erasure_hdfs_path, "erasure", 0)
        prev_erasure_path = erasure_hdfs_path

    if prev_product_path != product_hdfs_path:
        produceData(product_hdfs_path, "product", 1)
        prev_product_path = product_hdfs_path

    if prev_customer_path != customer_hdfs_path:
        produceData(customer_hdfs_path, "customer", 2)
        prev_customer_path = customer_hdfs_path

    if prev_transaction_path != transact_hdfs_path:
        produceData(transact_hdfs_path, "transaction", 3)
        prev_transaction_path = transact_hdfs_path
    
    sleep(600)
    today_date = "2020-01-01" #getDateTime()[0]
    today_hr = getDateTime()[1]
    today_min = getDateTime()[2]
    today_sec = getDateTime()[3]


