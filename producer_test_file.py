# pylint: disable-all

import pandas as pd
import json

from time import sleep
from kafka import KafkaConsumer, KafkaProducer

producer = KafkaProducer(
    bootstra_servers=["52.91.57.73"],
    value_serializer=lambda x: json.dumps(x).encode('utf-8')
)

producer.send('kafka-test-action', value="{'Data':'Passing data to Kafka using Python.'}")