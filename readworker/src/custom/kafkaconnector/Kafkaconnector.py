import logging
from icecream import ic
import configparser
from confluent_kafka import Consumer, KafkaException

#set the logging level #todo move to config
logging.basicConfig(level=logging.DEBUG)

class Kafkaconnector():
    def __init__(self) -> None:
        #set the logging level #todo move to config
        logging.basicConfig(level=logging.DEBUG)

        config = configparser.ConfigParser()
        config.read('app/consumer.ini', encoding='utf-8')

        self.kafkaAddress = config['kafka']['address']
        self.kafkaTopic = config['kafka']['topic']
        self.kafkaTopic = self.__unravelList()
        self.kafkaConsumerGroup = config['kafka']['consumergroup']

        self.conf = {'bootstrap.servers': self.kafkaAddress,
        'group.id': self.kafkaConsumerGroup,
        'auto.offset.reset': 'smallest'}

        self.consumer = Consumer(self.conf)
    
    
    def __unravelList(self):
        returnList = []
        for kv in self.kafkaTopic.split(","):
            returnList.append(str(kv))
        logging.debug(ic(returnList))
        return returnList
    
    def connect(self):
        running = True
        try:
            #consumer.subscribe(['testTopic'])
            logging.debug(self.kafkaTopic)
            self.consumer.subscribe(self.kafkaTopic)
            logging.debug('subscribed to topic')

            while running:
                msg = self.consumer.poll(timeout=1.0)
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
            self.consumer.close()
            logging.debug('closed kafka connection')