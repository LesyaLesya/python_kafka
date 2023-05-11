import pytest

from kafka_producer import (get_producer, publish_message)
from kafka_consumer import (get_consumer, listen)


@pytest.mark.parametrize('topic, msg', [('test', 'SuperCool'),
                                        ('hello', {'hello': 123}),
                                        ('test', [1, 2, 3, 'hello'])])
def test_kafka_messages(topic, msg):
    publish_message(get_producer(), topic, msg)
    result = listen(get_consumer(topic))
    assert result['topic'] == topic, f'Topic - {result["topic"]}'
    assert result['message'] == msg, f'Message - {result["message"]}'
