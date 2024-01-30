from confluent_kafka import Consumer, KafkaException
from icecream import ic
import time
import logging
import configparser

def unravelList(kafaTopic):
    returnList = []
    for kv in kafaTopic.split(","):
        returnList.append(str(kv))
    logging.debug(ic(returnList))
    return returnList

#set the logging level #todo move to config
logging.basicConfig(level=logging.DEBUG)

config = configparser.ConfigParser()
config.read('app/consumer.ini', encoding='utf-8')


kafkaAddress = config['kafka']['address']
kafkaTopic = config['kafka']['topic']
kafkaTopic = unravelList(kafkaTopic)
kafkaConsumerGroup = config['kafka']['consumergroup']


conf = {'bootstrap.servers': kafkaAddress,
        'group.id': kafkaConsumerGroup,
        'auto.offset.reset': 'smallest'}

def basic_consume_loop(consumer, topic, running):
    try:
        #consumer.subscribe(['testTopic'])
        logging.debug(topic)
        consumer.subscribe(topic)
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