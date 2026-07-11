from confluent_kafka import Consumer
from confluent_kafka.serialization import SerializationContext, MessageField, StringDeserializer
from confluent_kafka.schema_registry import SchemaRegistryClient
from confluent_kafka.schema_registry.json_schema import JSONDeserializer
import os
import logging
import time
import sys

# Десериализация объекта из dict
def message_from_dict(message, ctx):
    return message

# Настройка логирования
logger = logging.getLogger("Kafka consumer")
logger.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s %(levelname)s: %(message)s')
# Для консоли
console_handler = logging.StreamHandler()
console_handler.setFormatter(formatter)
logger.addHandler(console_handler)

# Считываем переменные, которые Docker передал внутрь контейнера
kafka_user = os.getenv('KAFKA_USER')
kafka_password = os.getenv('KAFKA_PASSWORD')
kafka_brokers = os.getenv('KAFKA_BROKERS')
schema_registry_urls = os.getenv('SCHEMA_REGISTRY_URLS')
auth_info = f"{kafka_user}:{kafka_password}"
ca_cert = "/app/certs/" + os.getenv('CA_CERT')
topic = os.getenv('KAFKA_TOPIC')

consumer_params = {
    'bootstrap.servers': kafka_brokers,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': ca_cert,
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': kafka_user,
    'sasl.password': kafka_password,
    'group.id': 'prod_consumer_group',
    'auto.offset.reset': 'earliest',
    'enable.auto.commit': False,
    'fetch.min.bytes': 1,
    'fetch.wait.max.ms': 100
}

sr_params = {
    'url': schema_registry_urls,
    'basic.auth.user.info': auth_info,
    'ssl.ca.location': ca_cert
}

#===================================================
if __name__ == "__main__":

    subject = topic + "-value" # Имя схемы для получения в Schema Registry - https://yandex.cloud/ru/docs/managed-kafka/concepts/managed-schema-registry
    # Инициализация консьюмера и подписка на топик
    consumer = Consumer(consumer_params)
    consumer.subscribe([topic])
    # Инициализация клиента Schema Registry
    schema_registry_client = SchemaRegistryClient(sr_params)

    # Проверка и ожидание готовности схемы
    ready = False
    while not ready:
        try:
            latest = schema_registry_client.get_latest_version(subject)
            json_schema_str = latest.schema.schema_str
            json_deserializer = JSONDeserializer(json_schema_str, from_dict=message_from_dict)
            ready = True
        except Exception as e:
            logger.error(f"Ошибка получения схемы для имени {subject}: {e}")
            logger.info(f"Повторная попытка запуска консьюмера - через 10 секунд")
            time.sleep(10)
#    try:
#        latest = schema_registry_client.get_latest_version(subject)
#        json_schema_str = latest.schema.schema_str
#        json_deserializer = JSONDeserializer(json_schema_str, from_dict=message_from_dict)
#    except Exception as e:
#        logger.error(f"Ошибка получения схемы для имени {subject}: {e}")
#        logger.error(f"Неуспешное завершение работы консьюмера")
#        sys.exit(1) # Завершение работы приложения

    # Определение десериализации ключа и значения
    key_deserializer = StringDeserializer('utf-8')
    value_deserializer = json_deserializer

    logger.info(f"Консьюмер начинает получение сообщений")
    try:
        while True:
            # Получение сообщения
            msg = consumer.poll(0.1)
            if msg is None:
                continue
            if msg.error():
                logger.error(f"Ошибка при получении: {msg.error()}")
                continue
            message_key = key_deserializer(msg.key(), SerializationContext(msg.topic(), MessageField.KEY))
            message_value = value_deserializer(msg.value(), SerializationContext(msg.topic(), MessageField.VALUE))
            logger.info(f"Получено сообщение: id={message_value['id']}, hostname={message_value['hostname']}, status={message_value['status']}, msg_body={message_value['msg_body']}")
            # Ручной коммит после обработки сообщения
            consumer.commit(msg, asynchronous=False)
    except Exception as e:
        logger.error(f"Произошла критическая ошибка: {e}")
    finally:
        consumer.close()
        logger.info(f"Консьюмер завершил работу")
