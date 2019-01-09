# Importing libraries
import socket
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
from kafka.errors import *
import configparser
from requests.exceptions import *


config = configparser.ConfigParser()
config.read('config.ini')


# Replace the values below with yours auth credentials
ACCESS_TOKEN = config['CREDENTIALS']['ACCESS_TOKEN']
ACCESS_SECRET = config['CREDENTIALS']['ACCESS_SECRET']
CONSUMER_KEY = config['CREDENTIALS']['CONSUMER_KEY']
CONSUMER_SECRET = config['CREDENTIALS']['CONSUMER_SECRET']
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# Kafka cluster details (single node cluster for dev)
bootstrap_server_list = ['XXX.XXX.XXX.XXX:9092']
TOPIC_NAME = config['DEFAULT']['TOPIC_NAME']


# This create a new kafka producer instance
def connect_kafka_producer():
    _producer = None
    try:
        _producer = KafkaProducer(bootstrap_servers=bootstrap_server_list, acks=1, api_version=(0, 10))
    except Exception as ex:
        print('Exception while connecting Kafka')
        print(str(ex))
    finally:
        return _producer


# This function iterates all the tweets and push them to kafka topic
def publish_message(producer_instance, topic_name, key, value):
    for line in value.iter_lines():
        if value is not None:
            try:
                key_bytes = bytes(key)
                try:
                    full_tweet = json.loads(line)
                except ValueError as e:
                    pass
                value_bytes = bytes(full_tweet['text'].encode('utf8', 'replace'))
                producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
                producer_instance.flush()
                print('Message published successfully.')
            except requests.exceptions.ConnectionError as e:
                print("Note able to send messages")


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '69.441691,7.947735, 97.317240,35.224256'), ('track', 'narendra,modi,namo,rahul,gandhi,raga')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = None
    try:
        response = requests.get(query_url, auth=my_auth, stream=True, timeout=5)
        print(query_url, str(response.status_code))
    except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError):
        print("Unable to get data from twitter")
    finally:
        return response


def app():
    print("Waiting for TCP connection...")

    producer = connect_kafka_producer()

    print("Connected... Starting getting tweets.")
    resp = get_tweets()
    try:
        publish_message(producer, TOPIC_NAME, "1", resp)  # We are mentioning KEY value as 1(Though it's not mandatory)
    except KeyboardInterrupt:
        print("Programme stopped by end user. Stopping.........")
    finally:
        producer.close()


if __name__ == "__main__":
    app()
