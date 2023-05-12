import json
from kafka import KafkaConsumer
from kafka.errors import KafkaError


def listen(topic, server='localhost', port='9094'):
    try:
        consumer = KafkaConsumer(
            topic, bootstrap_servers=f'{server}:{port}', auto_offset_reset='earliest',
            consumer_timeout_ms=7000)
        print('Connected consumer')
    except KafkaError:
        return f'Oops, something wet wrong in consumer {server}:{port}'
    result = dict()
    try:
        msgs = [[i.topic, i.value.decode('utf-8')] for i in consumer][-1]
    except KafkaError:
        return 'Cannot fetch messages'

    result['topic'] = msgs[0]
    try:
        result['message'] = json.loads(msgs[1])
    except:
        result['message'] = msgs[1]
    print(f'Received message in topic {result["topic"]}: {result["message"]}')
    return result
