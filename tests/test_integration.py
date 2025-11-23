"""
Integration tests for the streaming pipeline.
Tests producers, consumers, and data flow.
"""

import pytest
import json
import time
from confluent_kafka import Producer, Consumer, KafkaError
from datetime import datetime
import uuid


@pytest.fixture(scope="module")
def kafka_config():
    """Kafka configuration for tests."""
    return {
        'bootstrap.servers': 'localhost:9092'
    }


@pytest.fixture
def kafka_producer(kafka_config):
    """Create a Kafka producer for testing."""
    producer = Producer(kafka_config)
    yield producer
    producer.flush()


@pytest.fixture
def kafka_consumer(kafka_config):
    """Create a Kafka consumer for testing."""
    consumer = Consumer({
        **kafka_config,
        'group.id': f'test-consumer-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False
    })
    yield consumer
    consumer.close()


def test_sensor_data_flow(kafka_producer, kafka_consumer):
    """Test that sensor data can be produced and consumed."""
    topic = 'sensor-readings'
    
    # Produce test message
    test_data = {
        'sensor_id': 'test_sensor_001',
        'sensor_type': 'temperature',
        'value': 25.5,
        'timestamp': datetime.utcnow().isoformat(),
        'location': 'test_zone',
        'anomaly': False,
        'metadata': {
            'battery_level': 95,
            'signal_strength': -45
        }
    }
    
    kafka_producer.produce(
        topic=topic,
        key='test_sensor_001',
        value=json.dumps(test_data)
    )
    kafka_producer.flush()
    
    # Consume message
    kafka_consumer.subscribe([topic])
    
    msg = None
    timeout = time.time() + 10  # 10 second timeout
    
    while time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg and not msg.error():
            break
    
    assert msg is not None, "No message received"
    assert not msg.error(), f"Kafka error: {msg.error()}"
    
    # Verify message content
    received_data = json.loads(msg.value().decode('utf-8'))
    assert received_data['sensor_id'] == test_data['sensor_id']
    assert received_data['sensor_type'] == test_data['sensor_type']
    assert received_data['value'] == test_data['value']


def test_ecommerce_events_flow(kafka_producer, kafka_consumer):
    """Test that e-commerce events can be produced and consumed."""
    topic = 'ecommerce-events'
    
    test_event = {
        'event_id': str(uuid.uuid4()),
        'event_type': 'purchase',
        'user_id': 'test_user_001',
        'timestamp': datetime.utcnow().isoformat(),
        'session_id': 'test_session',
        'device': 'desktop',
        'ip_address': '127.0.0.1',
        'product_id': 123,
        'product_name': 'Test Product',
        'quantity': 2,
        'price': 29.99,
        'total': 59.98
    }
    
    kafka_producer.produce(
        topic=topic,
        key=test_event['user_id'],
        value=json.dumps(test_event)
    )
    kafka_producer.flush()
    
    kafka_consumer.subscribe([topic])
    
    msg = None
    timeout = time.time() + 10
    
    while time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg and not msg.error():
            break
    
    assert msg is not None
    assert not msg.error()
    
    received_event = json.loads(msg.value().decode('utf-8'))
    assert received_event['event_id'] == test_event['event_id']
    assert received_event['event_type'] == test_event['event_type']
    assert received_event['user_id'] == test_event['user_id']


def test_multiple_messages_ordering(kafka_producer, kafka_consumer):
    """Test that multiple messages maintain order within a partition."""
    topic = 'sensor-readings'
    sensor_id = 'test_sensor_002'
    num_messages = 10
    
    # Produce multiple messages with same key (same partition)
    for i in range(num_messages):
        data = {
            'sensor_id': sensor_id,
            'sensor_type': 'temperature',
            'value': 20.0 + i,
            'timestamp': datetime.utcnow().isoformat(),
            'location': 'test_zone',
            'anomaly': False,
            'metadata': {
                'battery_level': 100,
                'signal_strength': -50
            }
        }
        kafka_producer.produce(
            topic=topic,
            key=sensor_id,
            value=json.dumps(data)
        )
    
    kafka_producer.flush()
    
    # Consume messages
    kafka_consumer.subscribe([topic])
    
    received_values = []
    timeout = time.time() + 15
    
    while len(received_values) < num_messages and time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            if data['sensor_id'] == sensor_id:
                received_values.append(data['value'])
    
    assert len(received_values) == num_messages
    # Verify ordering
    assert received_values == sorted(received_values)


@pytest.mark.parametrize("sensor_type,expected_range", [
    ("temperature", (-20, 80)),
    ("humidity", (0, 100)),
    ("pressure", (900, 1100))
])
def test_sensor_value_ranges(sensor_type, expected_range):
    """Test that sensor values are within expected ranges."""
    from producers.sensor_producer import SensorProducer
    
    producer = SensorProducer()
    data = producer.generate_sensor_data(0, sensor_type)
    
    min_val, max_val = expected_range
    assert min_val <= data['value'] <= max_val, \
        f"{sensor_type} value {data['value']} outside range {expected_range}"
