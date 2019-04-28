__author__ = "Soumyadip"

import requests
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from requests.exceptions import *
from requests import Response
from typing import Union

from src.main.python.configuration import Configuration
from src.main.python.errors import UnableToConnectTwitterError, BlankResponseError, SendMessageError
from src.main.python.strings import *


def connect_kafka_producer() -> KafkaProducer:
    """Creates a new Kafka producer
    params: None
    return: A KafkaProducer object
    """
    _producer = None
    while _producer is None:

        try:
            _producer = KafkaProducer(bootstrap_servers=Configuration.BOOTSTRAP_SERVER_LIST,
                                      client_id="tweet_receiver", retries=1, acks=1
                                      )
            return _producer
        except KafkaError as ex:
            print(get_text("kafka_timeout_error"))

        time.sleep(5)  # Time gap between two request


def get_tweets() -> Union[Response, None]:
    """This functions collects real time tweets from Twitter
    params: None
    return: Response object on None
    """
    url = Configuration.TWITTER_API_URL
    query_data = [('language', 'en'), ('locations', '69.441691,7.947735, 97.317240,35.224256'),
                  ('track', 'narendra,modi,namo,rahul,gandhi,raga')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    _response = None
    try:
        _response = requests.get(query_url, auth=Configuration.MY_AUTH, stream=True, timeout=5)
        print(query_url, str(_response.status_code))
    except (ConnectTimeout, HTTPError, ReadTimeout, Timeout, ConnectionError):
        print(get_text("twitter_connection_error"))
    except UnableToConnectTwitterError as e:
        print(get_text("internal_server_error"))

    return _response


def publish_message(producer_instance: KafkaProducer, topic_name: str, value: Union[Response, None], key="1") -> None:
    """This function pushes data to kafka topic
    :param producer_instance: Kafka producer object.
    :param topic_name: Name of the topic
    :param key: Value of the key of topic name. (Optional)
    :param value: Value which we want to send to Kafka topic
    :return:
    """
    while True:
        if producer_instance is None:
            producer_instance = connect_kafka_producer()
            time.sleep(5)  # Time gap between two request
        try:
            for line in value.iter_lines():
                if value is not None:
                    key_bytes = bytearray(key, 'utf-8')
                    try:
                        full_tweet = json.loads(line)
                        value_bytes = bytes(full_tweet['text'].encode('utf8', 'replace'))
                        producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
                        producer_instance.flush(timeout=2)
                    except KafkaError as e:
                        print(get_text("unable_to_send_error"))
        except BlankResponseError:
            print(get_text("no_message_received_error"))
            continue
        except Exception:
            print(get_text("internal_server_error"))
            print(get_text("reconnecting"))
            time.sleep(5)
            value = get_tweets()


def app():
    """Main function running the app
    :return: None
    """
    print(get_text("starting_app"))

    producer = connect_kafka_producer()
    print(get_text("connected_to_kafka"))

    resp = get_tweets()
    print(get_text("getting_tweets"))

    try:
        publish_message(producer, Configuration.TOPIC_NAME, resp)
    except KeyboardInterrupt:
        print(get_text("keyboard_interrupt"))
    except Exception:
        print(get_text("internal_server_error"))
    finally:
        producer.close()
