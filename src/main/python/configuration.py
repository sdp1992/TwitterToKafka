__author__ = "Soumyadip"

import os
import requests_oauthlib


class Configuration:
    # Setting credentials for the app
    ACCESS_TOKEN = os.environ['ACCESS_TOKEN']
    ACCESS_SECRET = os.environ['ACCESS_SECRET']
    CONSUMER_KEY = os.environ['CONSUMER_KEY']
    CONSUMER_SECRET = os.environ['CONSUMER_SECRET']

    # Kafka cluster details
    TOPIC_NAME = os.environ['TOPIC_NAME']
    BOOTSTRAP_SERVER_LIST = os.environ['BOOTSTRAP_SERVER_LIST']

    # Twitter Streaming API URL
    TWITTER_API_URL = os.environ['TWITTER_API_URL']

    DEBUG = os.environ['DEBUG']

    MY_AUTH = requests_oauthlib.OAuth1(CONSUMER_KEY, CONSUMER_SECRET,
                                       ACCESS_TOKEN, ACCESS_SECRET
                                       )
