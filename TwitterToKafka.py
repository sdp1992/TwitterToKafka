# Importing libraries
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
from kafka.errors import *
import configparser
from requests.exceptions import *
import time

config = configparser.ConfigParser()
config.read('config.ini')

# Replace the values below with yours auth credentials / Change config.ini file
ACCESS_TOKEN = config['CREDENTIALS']['ACCESS_TOKEN']
ACCESS_SECRET = config['CREDENTIALS']['ACCESS_SECRET']
CONSUMER_KEY = config['CREDENTIALS']['CONSUMER_KEY']
CONSUMER_SECRET = config['CREDENTIALS']['CONSUMER_SECRET']
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# Kafka cluster details (single node cluster)
bootstrap_server_list = ['127.0.0.1:9092']
TOPIC_NAME = config['DEFAULT']['TOPIC_NAME']


# This create a new kafka producer instance
def connect_kafka_producer():
    _producer = None
    while _producer is None:
        try:
            _producer = KafkaProducer(bootstrap_servers=bootstrap_server_list, client_id = "tweet_receiver",
                                      retries=1, acks=1)

            if _producer is None:
                raise Exception
        except Exception as ex:
            print('Unable to connect to Kafka.')
            pass
        else:
            return _producer


# This function iterates all the tweets and push them to kafka topic
def publish_message(producer_instance, topic_name, key, value):
    while True:
        try:
            for line in value.iter_lines():
                if value is not None:
                    key_bytes = bytes(key)
                    try:
                        full_tweet = json.loads(line)
                        value_bytes = bytes(full_tweet['text'].encode('utf8', 'replace'))
                        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
                        producer_instance.flush(timeout=2)
                    except KafkaTimeoutError as e:
                        time.sleep(10)
                        print("Unable to send messages.")
                    except Exception as e:
                        time.sleep(10)
                        print("Unable to process current message batch.")
        except Exception:
            print("No messages received.")
            time.sleep(10)
            app()


def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '69.441691,7.947735, 97.317240,35.224256'), ('track', 'narendra,modi,namo,rahul,gandhi,raga')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = None
    try:
        response = requests.get(query_url, auth=my_auth, stream=True, timeout=5)
        print(query_url, str(response.status_code))
    except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError):
        print("Unable to get data from twitter.")
    except Exception as e:
        print("Something wrong happened.")
    finally:
        return response


def app():
    print("Starting the app...")

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
