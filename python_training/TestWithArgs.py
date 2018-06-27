import json
import time
from kafka import KafkaProducer
from kafka.errors import KafkaError
from kafka.future import log
import random
from random import randint
import datetime


producer = KafkaProducer(bootstrap_servers= 'localhost:9092', value_serializer=lambda v: json.dumps(v).encode('utf-8'))
users = ["1", "2", "3", "4", "5"]
# future = producer.send('topic1', {"test": "test"})
# try:
#     record_metadata = future.get(timeout=20)
# except KafkaError:
#     log.exeption()
#     pass
#
# print(record_metadata.topic)
# print(record_metadata.partition)
# print(record_metadata.offset)
while True:
    # producer.send('topic1', {"timestamp": str(datetime.datetime.utcnow().strftime('%B %d %Y - %H:%M:%S')),
    #                          "user": str(random.choice(users))})
    producer.send('topic1', {"timestamp": str(datetime.datetime.utcnow().strftime('%B %d %Y - %H:%M:%S')),
                             "user": "A"})
    time.sleep(2)
    producer.send('topic1', {"timestamp": str(datetime.datetime.utcnow().strftime('%B %d %Y - %H:%M:%S')),
                             "user": "B"})
    time.sleep(2)
    producer.send('topic1', {"timestamp": str(datetime.datetime.utcnow().strftime('%B %d %Y - %H:%M:%S')),
                             "user": "A"})
    time.sleep(4)