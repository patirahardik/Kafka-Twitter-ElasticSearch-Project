import tweepy as tw
from json import dumps
from configparser import ConfigParser
from kafka import KafkaProducer
import logging
import pprint
from time import sleep


# Setting default logging
logging.basicConfig(level=logging.INFO)

# Getting Keys for Twitter application
config = ConfigParser()
config.read('application.properties')
consumer_key = config.get("DEV", "consumer.key")
consumer_secret_key = config.get("DEV", "consumer.secret.key")
access_key = config.get("DEV", "access.key")
access_secret_key = config.get("DEV", "access.secret.key")


# Authenticating with Twitter
auth = tw.OAuthHandler(consumer_key, consumer_secret_key)
auth.set_access_token(access_key, access_secret_key)
api = tw.API(auth, wait_on_rate_limit=True)

search_words = 'Modi'

tweets = tw.Cursor(api.search,
                   q=search_words,
                   lang="en").items()

#
producer = KafkaProducer(bootstrap_servers=['127.0.0.1:9092'],
                         value_serializer=lambda x: dumps(x).encode('utf-8'),
                         acks='all',  # Required for safe producer
                         retries=999999999999,  # Required for safe producer
                         max_in_flight_requests_per_connection=5,  # Required for safe producer
                         linger_ms=20,  # Required for high throughput producer
                         batch_size=32*1024)  # Required for high throughput producer

# Compression best snappy,lz4... but test all other compression

# Getting Producer's Configuration
pprint.pprint(producer.config)

# Getting Tweets
for i in tweets:
    print(i._json)
    producer.send('Kafka_Tweets', i._json)
    sleep(10)


producer.flush()
producer.close()
