from kafka import KafkaConsumer
from threading import Thread
import logging
import os
import sys
import time

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

FORMATTER = logging.Formatter("%(asctime)s — %(name)s — %(levelname)s — %(message)s")

console_handler = logging.StreamHandler(sys.stdout)
console_handler.setFormatter(FORMATTER)
logger.addHandler(console_handler)

brokers = os.environ.get('KAFKA_CONSUMER_BROKERS', 'kafka:9092')
brokers = brokers.split(",")

topic = os.environ.get('KAFKA_TOPIC', 'ml-train')
group_id = os.environ.get('KAFKA_GROUP_ID', 'ml-group')
offset_reset = os.environ.get('KAFKA_OFFSET_RESET', 'earliest')

class TesteDeThread(Thread):

    def __init__(self, message):
        super().__init__()
        self.message = message
    def run(self):
        time.sleep(60)
        logger.info(self.message)

def leitura():
    consumer = KafkaConsumer(topic,
                             bootstrap_servers=brokers,
                             auto_offset_reset=offset_reset,
                             enable_auto_commit=True,
                             group_id=group_id,
                             value_deserializer=lambda x: x.decode('utf-8'))

    for message in consumer:
        logger.info("Inicio")
        t = TesteDeThread(message)
        t.start()
        logger.info("Fim")

if __name__ == '__main__':
    leitura()