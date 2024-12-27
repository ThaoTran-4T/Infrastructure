#!/bin/python
import json
from kafka import KafkaConsumer
from dotenv import load_dotenv
import os
load_dotenv()

secret = os.getenv('secret')


consumer = KafkaConsumer(
    'dp-event-hub',
    bootstrap_servers=['dp-event-hub.servicebus.windows.net:9093'],
    value_deserializer=lambda v: json.loads(v.decode('utf-8')),
    security_protocol='SASL_SSL',
    sasl_mechanism='PLAIN',
    sasl_plain_username='$ConnectionString',
    sasl_plain_password=secret,
)


print("Start consume")

for message in consumer:
    # message value and key are raw bytes -- decode if necessary!
    # e.g., for unicode: `message.value.decode('utf-8')`
    print ("%s:%d:%d: key=%s value=%s" % (message.topic, message.partition,
                                          message.offset, message.key,
                                          message.value))
