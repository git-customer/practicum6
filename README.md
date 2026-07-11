# Общее описание

Данная инструкция описывает создание и настройку компонентов для работы в облачной среде Yandex Cloud.  
Состав:  
- кластер Kafka на базе решения "Managed Service for Kafka"
- кластер Hadoop на базе решения "Yandex Data Processing"
- виртуальная машина на базе решения "Compute Cloud" для запуска приложений на Python, работающих внутри контейнеров docker 


# Развёртывание и настройка Kafka-кластера в Yandex Cloud

Необходимо создать кластер с 3 брокерами по инструкции: https://yandex.cloud/ru/docs/managed-kafka/quickstart  
При создании указать галочку "Реестр схем данных".  
С целью экономии бюджета все хосты заказываются в минимальной конфигурации.  
Пример конфигурации приведён в папке cfg.  
После создания кластера требуется создать топик production-topic, а также пользователей для работы приложения Producer.py, Consumer.py, Hadoop.py с соответствующими правами. Примеры настроек для топика и пользователей приведены в папке screenshots.


# Развёртывание и настройка Hadoop-кластера в Yandex Cloud

Необходимо создать кластер по инструкции: https://yandex.cloud/ru/docs/data-proc/quickstart  
Обязательными компонентами будут являться HDFS, MAPREDUCE, YARN.  
С целью экономии бюджета все хосты заказываются в минимальной конфигурации.  
Пример конфигурации приведён в папке cfg.  
Перед созданием кластера также может потребоваться настройка NAT-шлюза для соответствующей подсети в Virtual Private Cloud.  



# Развёртывание и настройка виртуальной машины в Yandex Cloud

Необходимо создать виртуальную машину по инструкции: https://yandex.cloud/ru/docs/compute/quickstart/quick-create-linux  
В качестве базового образа рекомендуется выбрать Ubuntu 24.04 LTS. 
С целью экономии бюджета виртуальная машина заказывается в минимальной конфигурации.  
Пример конфигурации приведён в папке cfg.  
Для работы с кластером Kafka и запуска приложений Python в docker необходимо выполнить первоначальную настройку:

1. Установить обновления.  
`sudo apt-get update && sudo apt-get upgrade`  
2. Скачать сертификаты Yandex Cloud.  
```
sudo mkdir -p /usr/local/share/ca-certificates/Yandex/ && \
sudo wget "https://storage.yandexcloud.net/cloud-certs/CA.pem" \
     --output-document /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt && \
sudo chmod 0655 /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
```  
3. Установить актуальную версию утилиты kafkactl.  
```
wget https://github.com/deviceinsight/kafkactl/releases/download/v5.19.0/kafkactl_5.19.0_linux_amd64.tar.gz
tar xzf kafkactl_5.19.0_linux_amd64.tar.gz kafkactl
sudo mv kafkactl /usr/local/bin
```  
4. Создать конфигурационный файл для утилиты kafkactl.  
```
cd ~/ && \
mkdir --parents .config/kafkactl && \
cd ~/.config/kafkactl && \
vi config.yml
--- Пример содержимого файла config.yml на основе данных созданного ранее кластера Kafka (замените FQDN на актуальные) ---
contexts:
  default:
    brokers:
    - rc1a-lhn8508kc3h46dmh.mdb.yandexcloud.net:9091
    - rc1b-c25es5k2q0n4h7el.mdb.yandexcloud.net:9091
    - rc1d-2idk47hgrqv4uo4e.mdb.yandexcloud.net:9091
    sasl:
      enabled: true
      username: admin-user
      password: ...
      mechanism: scram-sha512
    tls:
      enabled: true
      ca: /usr/local/share/ca-certificates/Yandex/YandexInternalRootCA.crt
---
```  
5. Установить docker по инструкции https://docs.docker.com/engine/install  
6. Создать файл .env с параметрами подключения к кластерам Kafka и Hadoop. За основу нужно взять файл .env-example 


# Запуск приложения на виртуальной машине

Приложение запускается на виртуальной машины из папки с основными файлами (docker-compose.yml и т.д.) при помощи команды:  
`sudo docker compose up --build`  

Примером успешного запуска будут логи в консоли об отправке и получении сообщений.  
![Screenshot 1](https://github.com/git-customer/practicum6/blob/9b5088a5f5f75cb52373246c911fd69d2ee581fc/screenshots/vm_console_logs_1.png)


# Проверки

Для проверки информации о топиках, группах и схемах можно использовать следующий набор команд (замените FQDN на актуальные):  
```
export $(grep -v '^#' .env | xargs)
kafkactl get topics
kafkactl get consumer-groups
echo $KAFKA_TOPIC
kafkactl describe topic $KAFKA_TOPIC
curl -k -u "$KAFKA_PRODUCER_USER:$KAFKA_PRODUCER_PASSWORD" https://rc1a-lhn8508kc3h46dmh.mdb.yandexcloud.net:443/subjects | jq
curl -k -u "$KAFKA_PRODUCER_USER:$KAFKA_PRODUCER_PASSWORD" -X GET https://rc1a-lhn8508kc3h46dmh.mdb.yandexcloud.net:443/subjects/production-topic-value/versions | jq
```

Пример успешного выполнения команд:  
![Screenshot 1](https://github.com/git-customer/practicum6/blob/9b5088a5f5f75cb52373246c911fd69d2ee581fc/screenshots/vm_console_logs_2.png)
