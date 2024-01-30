import json
from kafka import KafkaProducer
import config

bootstrap_server = config.SERVER


# kafkaProdycer @return - produce serialized value and key to  server topic

def producer():
    return KafkaProducer(bootstrap_servers = [bootstrap_server],
                         value_serializer = lambda x: json.dumps(x).encode("utf-8"),
                         key_serializer = lambda x: x.encode("utf-8")
                         )

# send_data @return - send data to topic
def send_data(topic_name, data, p_key, partition_num):
    return producer().send(topic=topic_name, key=p_key, value=data, partition=partition_num)