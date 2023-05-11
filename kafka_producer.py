import json
from kafka import KafkaProducer
from kafka.errors import KafkaError


def get_producer(server='localhost', port='9092'):
    try:
        producer = KafkaProducer(bootstrap_servers=f'{server}:{port}')
        print('Connected producer')
        return producer
    except KafkaError:
        return f'Oops, something wet wrong in producer on server {server}:{port}'


def publish_message(producer, topic, message):
    if not isinstance(message, (dict, list)):
        message = bytes(message, encoding='utf-8')
    else:
        message = json.dumps(message).encode('utf-8')
    try:
        producer.send(topic, message)
        producer.flush()
        print(f'Topic {topic}: published message - {message} ')
    except KafkaError:
        return f'Cannot send message {message} to topic {topic}'
    return message
