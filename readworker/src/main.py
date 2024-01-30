
from icecream import ic
import time
import logging
from custom.kafkaconnector.Kafkaconnector import Kafkaconnector

loop = Kafkaconnector()

eternity = True
time.sleep(20)
while eternity:
    logging.info('doing a run')
    loop.connect()
    logging.info('taking a 10 sec break')
    time.sleep(10)
    