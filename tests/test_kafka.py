import pytest

from kafka_producer import publish_message
from kafka_consumer import listen


@pytest.mark.parametrize('topic, msg', [('test', 'SuperCool'),
                                        ('hello', {'hello': 123}),
                                        ('test', [1, 2, 3, 'hello'])])
def test_kafka_messages(topic, msg):
    publish_message(topic, msg)
    result = listen(topic)
    assert result['topic'] == topic, f'Topic - {result["topic"]}'
    assert result['message'] == msg, f'Message - {result["message"]}'


@pytest.mark.parametrize('topic, msg, host, port',
                         [('test', 'SuperCool', 'localhost', 9090),
                          ('test', 'SuperCool', '191.168.16.1', 9094)])
def test_kafka_invalid_producer(topic, msg, host, port):
    message = publish_message(topic, msg, server=host, port=port)
    assert message == f'Oops, something wet wrong in producer on server {host}:{port}', \
        f'Message - {message}'


@pytest.mark.parametrize('topic, msg, host, port',
                         [('test', 'SuperCool', 'localhost', 9090),
                          ('test', 'SuperCool', '191.168.16.1', 9094)])
def test_kafka_invalid_consumer(topic, msg, host, port):
    publish_message(topic, msg)
    result = listen(topic, server=host, port=port)
    assert result == f'Oops, something wet wrong in consumer {host}:{port}', \
        f'Message - {result}'
