from confluent_kafka import Consumer, KafkaError, KafkaException
from threading import Thread
from json import loads
import traceback
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

# Ambos os valores serão separado por virgula
topic = os.environ.get('KAFKA_TOPIC', 'ml-train')
group_id = os.environ.get('KAFKA_GROUP_ID', 'ml-group')
offset_reset = os.environ.get('KAFKA_OFFSET_RESET', 'earliest')

conf = {'bootstrap.servers': 'localhost',
        'group.id': group_id,
        'auto.offset.reset': offset_reset,
        'max.poll.interval.ms': 600000}

consumer = Consumer(conf)
running = True

def TesteDeThread(message):
        time.sleep(10)
        logger.info(message)

def leitura():
    try:
        consumer.subscribe([topic])

        while running:
            message = consumer.poll(timeout=-1)
            if message is None:
                continue

            if message.error():
                if message.error().code() == KafkaError._PARTITION_EOF:
                    # End of partition event
                    logger.error('%% %s [%d] fim do offset %d\n' %
                                 (message.topic(), message.partition(), message.offset()))
                    continue
                elif message.error():
                    raise KafkaException(message.error())
            else:
                logger.debug("Inicio do treinamento")
                try:
                    message = loads(message.value().decode('utf-8'))
                except Exception as e:
                    logger.error('Mensagem invalida: {}'.format(message))
                    continue

                try:
                    logger.info("Inicio")
                    TesteDeThread(message)
                    logger.info("Fim")

                except Exception as e:
                    stack_trace_string = traceback.format_exc()
                    logger.error('Erro na execução do treinamento. motivo {}\n{}'.format(str(e), stack_trace_string))

                logger.debug('Treinamento concluido!')
    finally:
        # Close down consumer to commit final offsets.
        consumer.close()

if __name__ == '__main__':
    leitura()