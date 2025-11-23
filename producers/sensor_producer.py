"""
IoT Sensor Data Producer
Simulates multiple IoT sensors sending temperature, humidity, and pressure data to Kafka.
"""

import json
import time
import random
from datetime import datetime, UTC
from typing import Dict, Any
from confluent_kafka import Producer
import yaml
import structlog

logger = structlog.get_logger()


class SensorProducer:
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize the sensor producer."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.producer = Producer({
            'bootstrap.servers': ','.join(self.config['kafka']['bootstrap_servers']),
            'client.id': 'sensor-producer'
        })
        
        self.topic = self.config['kafka']['topics']['sensor_data']
        self.num_sensors = self.config['producers']['sensor']['num_sensors']
        self.interval = self.config['producers']['sensor']['interval_seconds']
        
    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error('Message delivery failed', error=str(err))
        else:
            logger.debug('Message delivered', 
                        topic=msg.topic(), 
                        partition=msg.partition(), 
                        offset=msg.offset())
    
    def generate_sensor_data(self, sensor_id: int, sensor_type: str) -> Dict[str, Any]:
        """Generate realistic sensor data."""
        base_values = {
            'temperature': 20.0,
            'humidity': 50.0,
            'pressure': 1013.25
        }
        
        variations = {
            'temperature': 30.0,
            'humidity': 40.0,
            'pressure': 50.0
        }
        
        # Add some randomness with occasional anomalies
        anomaly = random.random() < 0.05  # 5% chance of anomaly
        
        if anomaly:
            value = base_values[sensor_type] + random.uniform(-2, 2) * variations[sensor_type]
        else:
            value = base_values[sensor_type] + random.uniform(-0.5, 0.5) * variations[sensor_type]
        
        return {
            'sensor_id': f'sensor_{sensor_id:03d}',
            'sensor_type': sensor_type,
            'value': round(value, 2),
            'timestamp': datetime.now(UTC).isoformat(),
            'location': f'zone_{sensor_id % 5}',
            'anomaly': anomaly,
            'metadata': {
                'battery_level': random.randint(20, 100),
                'signal_strength': random.randint(-80, -30)
            }
        }
    
    def produce(self):
        """Continuously produce sensor data."""
        logger.info("Starting sensor producer", 
                   num_sensors=self.num_sensors,
                   interval=self.interval)
        
        try:
            while True:
                for sensor_id in range(self.num_sensors):
                    for sensor_type in self.config['producers']['sensor']['sensor_types']:
                        data = self.generate_sensor_data(sensor_id, sensor_type)
                        
                        # Produce message to Kafka
                        self.producer.produce(
                            topic=self.topic,
                            key=str(sensor_id),
                            value=json.dumps(data),
                            callback=self.delivery_report
                        )
                
                # Trigger delivery reports
                self.producer.poll(0)
                
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            # Wait for any outstanding messages to be delivered
            self.producer.flush()


if __name__ == '__main__':
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    )
    
    producer = SensorProducer()
    producer.produce()
