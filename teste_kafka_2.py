from confluent_kafka import Consumer, KafkaError, KafkaException
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
topic = os.environ.get('KAFKA_TOPIC', 'ml-train')
group_id = os.environ.get('KAFKA_GROUP_ID', 'ml-group')
offset_reset = os.environ.get('KAFKA_OFFSET_RESET', 'smallest')

conf = {'bootstrap.servers': brokers,
        'group.id': group_id,
        'auto.offset.reset': offset_reset}

consumer = Consumer(conf)

#class TesteDeThread(Thread):
class TesteDeThread():

    def __init__(self, message):
        super().__init__()
        self.message = message

    def run(self):
        time.sleep(60)
        logger.info(self.message)

running = True

def basic_consume_loop(consumer, topics):
    try:
        consumer.subscribe(topics)

        while running:
            msg = consumer.poll(timeout=-1)
            if msg is None:
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    sys.stderr.write('%% %s [%d] reached end at offset %d\n' %
                                     (msg.topic(), msg.partition(), msg.offset()))
                elif msg.error():
                    raise KafkaException(msg.error())
            else:
                logger.info("Inicio")
                t = TesteDeThread(msg)
                t.run()
                logger.info("Fim")
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

def shutdown():
    running = False

if __name__ == '__main__':
    basic_consume_loop(consumer, [topic])