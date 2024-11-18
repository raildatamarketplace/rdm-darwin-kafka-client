# Code snippet for Kafka client in Python

from confluent_kafka import Consumer
from pathlib import Path
import argparse
import warnings
warnings.filterwarnings('ignore')
import json

BOOTSTRAP_SERVER = ''
GROUP_ID = ''
SASL_USERNAME = ''
SASL_PASSWORD = ''
SSL_CA_LOCATION = ''
TOPIC_NAME = ''

config = {
    'bootstrap.servers': BOOTSTRAP_SERVER,
    'group.id': GROUP_ID,
    'security.protocol': 'SASL_SSL',
    'sasl.mechanism': 'PLAIN',
    'sasl.username': SASL_USERNAME,
    'sasl.password': SASL_PASSWORD,
    'ssl.ca.location': SSL_CA_LOCATION,
    'auto.offset.reset': 'earliest'
}

consumer = Consumer(config)
consumer.subscribe([TOPIC_NAME])

l = []
count = 0
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is not None and msg.error() is None:
            key = msg.key()
            value = msg.value()
            try:
                if key is not None:
                    key = key.decode('utf-8')
            except:
                key = "None"
            try:
                if value is not None:
                    value = value.decode('utf-8')
            except:
                value = "None"
            # print("key = {key:12} value = {value:12}".format(key, value))
            print(key, value)
            l.append(value)
            count+=1
            print(count)
except KeyboardInterrupt:
    pass
finally:
    consumer.close()

