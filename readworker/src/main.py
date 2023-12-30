
from confluent_kafka import Consumer, KafkaException
import time
import sys
import logging

kafkaTopic = ['testTopic']
kafkaAddress = 'redpanda-0:9092'
kafkaConsumerGroup = 'mygroup'
logging.basicConfig(level=logging.DEBUG)

conf = {'bootstrap.servers': kafkaAddress,
        'group.id': kafkaConsumerGroup,
        'auto.offset.reset': 'smallest'}

def basic_consume_loop(consumer, topics, running):
    try:
        consumer.subscribe(topics)
        logging.debug('subscribed to topic')

        while running:
            msg = consumer.poll(timeout=1.0)
            if msg is None: continue

            if msg.error():
                raise KafkaException(msg.error())
            else:
                key = msg.key().decode('utf-8')
                value = msg.value().decode('utf-8')
                logging.debug(key)
                logging.debug(value)
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()
        logging.debug('closed kafka connection')

eternity = True
running = False
while eternity:
    logging.info('doing a run')
    consumer = Consumer(conf)
    basic_consume_loop(consumer, kafkaTopic, running)
    running = True
    time.sleep(10)