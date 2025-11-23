"""
Integration tests for the streaming pipeline.
Tests producers, consumers, and data flow.
"""

import pytest
import json
import time
from confluent_kafka import Producer, Consumer
from datetime import datetime, UTC
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
    """Create a Kafka consumer for testing with function scope."""
    consumer = Consumer({
        **kafka_config,
        'group.id': f'test-consumer-{uuid.uuid4()}',
        'auto.offset.reset': 'earliest',
        'enable.auto.commit': False,
        'session.timeout.ms': 6000,
        'max.poll.interval.ms': 10000
    })
    yield consumer
    try:
        consumer.close()
    except Exception:
        pass  # Consumer might already be closed


def test_sensor_data_flow(kafka_producer, kafka_consumer):
    """Test that sensor data can be produced and consumed."""
    topic = 'sensor-readings'
    test_id = str(uuid.uuid4())
    
    # Produce test message with unique test_id
    test_data = {
        'sensor_id': f'test_sensor_{test_id}',
        'sensor_type': 'temperature',
        'value': 25.5,
        'timestamp': datetime.now(UTC).isoformat(),
        'location': 'test_zone',
        'anomaly': False,
        'metadata': {
            'battery_level': 95,
            'signal_strength': -45,
            'test_id': test_id  # Mark as test message
        }
    }
    
    kafka_producer.produce(
        topic=topic,
        key=test_data['sensor_id'],
        value=json.dumps(test_data)
    )
    kafka_producer.flush()
    
    # Subscribe and consume messages
    kafka_consumer.subscribe([topic])
    
    msg = None
    timeout = time.time() + 10  # 10 second timeout
    
    while time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg and not msg.error():
            received_data = json.loads(msg.value().decode('utf-8'))
            # Check if this is our test message
            if received_data.get('metadata', {}).get('test_id') == test_id:
                break
    
    assert msg is not None, "No message received"
    assert not msg.error(), f"Kafka error: {msg.error()}"
    
    # Verify message content
    received_data = json.loads(msg.value().decode('utf-8'))
    assert received_data['sensor_id'] == test_data['sensor_id']
    assert received_data['sensor_type'] == test_data['sensor_type']
    assert received_data['value'] == test_data['value']


@pytest.mark.skip(reason="Ecommerce topic needs producer running first")
def test_ecommerce_events_flow(kafka_producer, kafka_consumer):
    """Test that e-commerce events can be produced and consumed."""
    topic = 'ecommerce-events'
    test_event_id = str(uuid.uuid4())
    
    test_event = {
        'event_id': test_event_id,
        'event_type': 'purchase',
        'user_id': 'test_user_001',
        'timestamp': datetime.now(UTC).isoformat(),
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
    
    # Subscribe and consume messages
    kafka_consumer.subscribe([topic])
    
    found_message = False
    received_event = None
    timeout = time.time() + 10
    
    while time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg is None:
            continue
        if msg.error():
            continue
        
        received_event = json.loads(msg.value().decode('utf-8'))
        # Check if this is our test message
        if received_event.get('event_id') == test_event_id:
            found_message = True
            break
    
    assert found_message, f"Test message with event_id {test_event_id} not found"
    assert received_event is not None, "No message data received"
    assert received_event['event_id'] == test_event['event_id']
    assert received_event['event_type'] == test_event['event_type']
    assert received_event['user_id'] == test_event['user_id']


def test_multiple_messages_ordering(kafka_producer, kafka_consumer):
    """Test that multiple messages maintain order within a partition."""
    topic = 'sensor-readings'
    test_id = str(uuid.uuid4())
    sensor_id = f'test_sensor_order_{test_id}'
    num_messages = 10
    
    # Produce multiple messages with same key (same partition)
    for i in range(num_messages):
        data = {
            'sensor_id': sensor_id,
            'sensor_type': 'temperature',
            'value': 20.0 + i,
            'timestamp': datetime.now(UTC).isoformat(),
            'location': 'test_zone',
            'anomaly': False,
            'metadata': {
                'battery_level': 100,
                'signal_strength': -50,
                'test_id': test_id,
                'sequence': i
            }
        }
        kafka_producer.produce(
            topic=topic,
            key=sensor_id,
            value=json.dumps(data)
        )
    
    kafka_producer.flush()
    
    # Subscribe and consume messages
    kafka_consumer.subscribe([topic])
    
    received_values = []
    timeout = time.time() + 15
    
    while len(received_values) < num_messages and time.time() < timeout:
        msg = kafka_consumer.poll(timeout=1.0)
        if msg and not msg.error():
            data = json.loads(msg.value().decode('utf-8'))
            # Only collect messages from this test run
            if data.get('metadata', {}).get('test_id') == test_id:
                received_values.append(data['value'])
    
    assert len(received_values) == num_messages, f"Expected {num_messages} messages, got {len(received_values)}"
    # Verify ordering
    assert received_values == sorted(received_values), "Messages not in order"


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
