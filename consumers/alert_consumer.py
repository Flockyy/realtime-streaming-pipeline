"""
Alert Consumer
Monitors sensor data for anomalies and threshold violations.
"""

import json
from datetime import datetime
from confluent_kafka import Consumer, Producer, KafkaError, KafkaException
import yaml
import structlog

logger = structlog.get_logger()


class AlertConsumer:
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize the alert consumer."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Consumer setup
        self.consumer = Consumer({
            'bootstrap.servers': ','.join(self.config['kafka']['bootstrap_servers']),
            'group.id': 'alert-consumer-group',
            'auto.offset.reset': self.config['kafka']['auto_offset_reset']
        })
        
        # Producer for alerts
        self.producer = Producer({
            'bootstrap.servers': ','.join(self.config['kafka']['bootstrap_servers'])
        })
        
        self.topic = self.config['kafka']['topics']['sensor_data']
        self.alert_topic = self.config['kafka']['topics']['alerts']
        self.consumer.subscribe([self.topic])
        
        self.thresholds = self.config['consumers']['alerts']['thresholds']
    
    def check_thresholds(self, message: dict) -> dict | None:
        """Check if message violates any thresholds."""
        sensor_type = message['sensor_type']
        value = message['value']
        
        alert = None
        
        if sensor_type == 'temperature':
            if value > self.thresholds['temperature_max']:
                alert = {
                    'type': 'threshold_violation',
                    'severity': 'critical',
                    'sensor_id': message['sensor_id'],
                    'sensor_type': sensor_type,
                    'value': value,
                    'threshold': self.thresholds['temperature_max'],
                    'message': f"Temperature {value}°C exceeds maximum threshold",
                    'timestamp': datetime.utcnow().isoformat()
                }
            elif value < self.thresholds['temperature_min']:
                alert = {
                    'type': 'threshold_violation',
                    'severity': 'warning',
                    'sensor_id': message['sensor_id'],
                    'sensor_type': sensor_type,
                    'value': value,
                    'threshold': self.thresholds['temperature_min'],
                    'message': f"Temperature {value}°C below minimum threshold",
                    'timestamp': datetime.utcnow().isoformat()
                }
        
        elif sensor_type == 'humidity':
            if value > self.thresholds['humidity_max']:
                alert = {
                    'type': 'threshold_violation',
                    'severity': 'warning',
                    'sensor_id': message['sensor_id'],
                    'sensor_type': sensor_type,
                    'value': value,
                    'threshold': self.thresholds['humidity_max'],
                    'message': f"Humidity {value}% exceeds maximum threshold",
                    'timestamp': datetime.utcnow().isoformat()
                }
        
        # Check for anomalies
        if message.get('anomaly', False):
            alert = {
                'type': 'anomaly_detected',
                'severity': 'info',
                'sensor_id': message['sensor_id'],
                'sensor_type': sensor_type,
                'value': value,
                'message': f"Anomaly detected in {sensor_type} reading",
                'timestamp': datetime.utcnow().isoformat()
            }
        
        return alert
    
    def send_alert(self, alert: dict):
        """Send alert to alerts topic."""
        self.producer.produce(
            topic=self.alert_topic,
            key=alert['sensor_id'],
            value=json.dumps(alert)
        )
        self.producer.poll(0)
        
        logger.warning("Alert generated",
                      sensor_id=alert['sensor_id'],
                      type=alert['type'],
                      severity=alert['severity'])
    
    def consume(self):
        """Main consumer loop."""
        logger.info("Starting alert consumer", topic=self.topic)
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Parse and check message
                message = json.loads(msg.value().decode('utf-8'))
                alert = self.check_thresholds(message)
                
                if alert:
                    self.send_alert(alert)
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == '__main__':
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    )
    
    consumer = AlertConsumer()
    consumer.consume()
