from elasticsearch import Elasticsearch, helpers
import configparser
from kafka import KafkaConsumer
import json
import logging


config = configparser.ConfigParser()
config.read("application.properties")
es_host = config.get("DEV", "elasticsearch.host")
client = Elasticsearch(es_host)

logging.basicConfig(level='INFO')

consumer = KafkaConsumer('Kafka_Tweets',
                         bootstrap_servers=['127.0.0.1:9092'],
                         auto_offset_reset='earliest',
                         value_deserializer=lambda x: json.loads(x.decode('ascii')),
                         consumer_timeout_ms=10000,
                         enable_auto_commit=False,
                         group_id='App-1')

# for messages in consumer:
#     print(messages.value)
#     res = client.index("twitter", messages.value, doc_type='tweets', id=messages.value['id_str'])
#     # Add custom id to elastic search to each record commit once, if it comes again it will be a update.
#     print(res['result'])
#     consumer.commit()


while True:

    msg_pack = consumer.poll(500)

    for tp, messages in msg_pack.items():
        print('Received ' + str(len(messages)) + " records")
        for message in messages:
            res = client.index("twitter", message.value, doc_type='tweets', id=message.value['id_str'])
            print(res['result'])

    if len(msg_pack):
        logging.info("Committing Offset...")
        consumer.commit()
        logging.info("Committed Successfully")


