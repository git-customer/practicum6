from confluent_kafka import Producer
from confluent_kafka.serialization import SerializationContext, MessageField, StringSerializer
from confluent_kafka.schema_registry import SchemaRegistryClient, Schema
from confluent_kafka.schema_registry.json_schema import JSONSerializer
import os
import logging
import time
import uuid

# Функция обратного вызова для ошибок подключения к Kafka
def error_callback(err):
    logger.error(f"Ошибка при подключении к Kafka: {err}")

# Функция обратного вызова для подтверждения доставки
def delivery_report(err, msg):
   if err is not None:
       logger.error(f"Сбой доставки сообщения: {err}")
   else:
       logger.info(f"Сообщение доставлено в топик {msg.topic()} и партицию {msg.partition()} со смещением {msg.offset()}")

# Сериализация объекта в dict
def message_to_dict(message, ctx):
    return message

# Настройка логирования
logger = logging.getLogger("Kafka producer")
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

# Конфигурация подключения к брокерам Kafka
producer_params = {
    'bootstrap.servers': kafka_brokers,
    'security.protocol': 'SASL_SSL',
    'ssl.ca.location': ca_cert,
    'sasl.mechanism': 'SCRAM-SHA-512',
    'sasl.username': kafka_user,
    'sasl.password': kafka_password,
    'error_cb': error_callback,
    'acks': 'all', # Гарантия доставки
    'retries': 5, # Повторы отправки при сетевых сбоях
    'retry.backoff.ms': 200 # Пауза между повторными попытками в мc
}

# Конфигурация подключения к Schema Registry
sr_params = {
    'url': schema_registry_urls,
    'basic.auth.user.info': auth_info,
    'ssl.ca.location': ca_cert
}

# Описание структуры данных JSON
JSON_SCHEMA_STR = """
{
 "$schema": "http://json-schema.org/draft-07/schema#",
 "title": "logs_message",
 "type": "object",
 "properties": {
   "id": { "type": "string" },
   "hostname": { "type": "string" },
   "status": { "type": "string" },
   "msg_body": { "type": "string" }
 },
 "required": ["id", "hostname", "status"]
}
"""
#===================================================
if __name__ == "__main__":

    subject = topic + "-value" # Имя схемы для регистрации в Schema Registry - https://yandex.cloud/ru/docs/managed-kafka/concepts/managed-schema-registry
    # Инициализация продюсера и клиента Schema Registry
    producer = Producer(producer_params)
    schema_registry_client = SchemaRegistryClient(sr_params)
    # Инициализация сериализатора
    json_serializer = JSONSerializer(JSON_SCHEMA_STR, schema_registry_client, message_to_dict)
    # Определение сериализации ключа и значения
    key_serializer = StringSerializer('utf-8')
    value_serializer = json_serializer

    try:
        # Попытка получить существующую схему, если она есть
        latest = schema_registry_client.get_latest_version(subject)
        logger.info(f"Схема уже зарегистрирована ранее для имени {subject}:\n{latest.schema.schema_str}")
    except Exception:
        # Регистрация схемы, если её нет
        schema_object = Schema(JSON_SCHEMA_STR, "JSON")
        schema_id = schema_registry_client.register_schema(subject, schema_object)
        logger.info(f"Успешно зарегистрирована схема для имени {subject} с id: {schema_id}")

    logger.info(f"Продюсер начинает отправку сообщений")
    try:
        # Отправка 5 сообщений с интервалом в 1с
        for i in range(1,6):
            # Формирование сообщения для отправки
            message_key = "host"+str(i)
            message_value = {"id": str(uuid.uuid4()), "hostname": "host"+str(i), "status": "SUCCESS", "msg_body": "Логи хоста host"+str(i)}
            # Отправка сообщения
            producer.produce(
                topic,
                key = key_serializer(message_key, SerializationContext(topic, MessageField.KEY)),
                value = value_serializer(message_value, SerializationContext(topic, MessageField.VALUE)),
                on_delivery = delivery_report
            )
            producer.poll(0)
            time.sleep(1)
    except Exception as e:
        logger.error(f"Произошла ошибка при отправке: {e}")
    finally:
        # Ожидание завершения отправки всех сообщений
        producer.flush()
        logger.info(f"Продюсер завершил работу")
