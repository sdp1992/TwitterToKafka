# Importing libraries
import socket
import requests
import requests_oauthlib
import json
from kafka import KafkaProducer
import datetime


# Replace the values below with yours auth credentials
ACCESS_TOKEN = '223106342-W71cOvvTyyTBTUwWgw4QBUybxDpysz20PUDVqDGB'
ACCESS_SECRET = '91PRaEhHIUmRhPN8SYS68vtminEuMcNKPaN7bsPcz8GpX'
CONSUMER_KEY = 'CuvYdTltOue5RFYc9oD17tkJq'
CONSUMER_SECRET = 'HYHthuLPZtqiBoRbQfDHhW94GYTKTIKBtxKtaiSgAbS3SP1EK1'
my_auth = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET, ACCESS_TOKEN, ACCESS_SECRET)

# Kafka cluster details (single node cluster for dev)
bootstrap_server_list = ['139.59.65.185:9092']


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
        try:
            key_bytes = bytes(key)
            print(line)
            full_tweet = json.loads(line)
            value_bytes = bytes(str(datetime.datetime.now()) + " >> " + full_tweet['text'].encode('utf8', 'replace'))
            producer_instance.send(topic_name, key=key_bytes, value=value_bytes)
            producer_instance.flush()
            print('Message published successfully.')
        except Exception as ex:
            print('Exception in publishing message')
            print(str(ex))


# This function helps getting real time tweets filtered by specified keywords
def get_tweets():
    url = 'https://stream.twitter.com/1.1/statuses/filter.json'
    query_data = [('language', 'en'), ('locations', '69.441691,7.947735, 97.317240,35.224256'), ('track', 'narendra,modi,namo,rahul,gandhi,raga')]
    query_url = url + '?' + '&'.join([str(t[0]) + '=' + str(t[1]) for t in query_data])
    response = requests.get(query_url, auth=my_auth, stream=True)
    print(query_url, str(response.status_code))
    return response


def main():
    tcp_ip = "localhost"
    tcp_port = 9009
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.bind((tcp_ip, tcp_port))
    s.listen(1)
    print("Waiting for TCP connection...")

    producer = connect_kafka_producer()

    print("Connected... Starting getting tweets.")
    resp = get_tweets()

    try:                                                   # Topic name: TwitterDataNaMoRaGa
        publish_message(producer, "TwitterDataNaMoRaGa", "1", resp) # We are mentioning KEY value as 1(Though it's not mandatory)
    except KeyboardInterrupt:
        print("Programme stopped by end user. Stopping.........")
    finally:
        producer.close()


if __name__ == "__main__":
    main()