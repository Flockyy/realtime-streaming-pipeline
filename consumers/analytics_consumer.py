"""
Analytics Consumer
Consumes sensor data, processes it, and stores aggregated results.
"""

import json
from datetime import datetime, timedelta
from typing import Dict, List
from collections import defaultdict
from confluent_kafka import Consumer, KafkaError, KafkaException
import yaml
import structlog
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
import pandas as pd

from utils.database import SensorReading, AggregatedMetrics
from utils.metrics import MetricsCollector

logger = structlog.get_logger()


class AnalyticsConsumer:
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize the analytics consumer."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        # Kafka consumer setup
        self.consumer = Consumer({
            'bootstrap.servers': ','.join(self.config['kafka']['bootstrap_servers']),
            'group.id': self.config['kafka']['consumer_group'],
            'auto.offset.reset': self.config['kafka']['auto_offset_reset'],
            'enable.auto.commit': self.config['kafka']['enable_auto_commit']
        })
        
        self.topic = self.config['kafka']['topics']['sensor_data']
        self.consumer.subscribe([self.topic])
        
        # Database setup
        db_config = self.config['postgres']
        db_url = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(db_url)
        self.Session = sessionmaker(bind=self.engine)
        
        # Metrics
        self.metrics = MetricsCollector()
        
        # Batch processing
        self.batch_size = self.config['consumers']['analytics']['batch_size']
        self.batch_timeout = self.config['consumers']['analytics']['batch_timeout_seconds']
        self.message_batch = []
        self.last_batch_time = datetime.utcnow()
        
        # Window for aggregations
        self.window_data = defaultdict(list)
        self.window_size = timedelta(
            seconds=self.config['processing']['windowing']['tumbling_window_seconds']
        )
    
    def process_message(self, message: Dict) -> SensorReading:
        """Process a single sensor reading."""
        return SensorReading(
            sensor_id=message['sensor_id'],
            sensor_type=message['sensor_type'],
            value=message['value'],
            timestamp=datetime.fromisoformat(message['timestamp']),
            location=message['location'],
            anomaly=message.get('anomaly', False),
            battery_level=message['metadata'].get('battery_level'),
            signal_strength=message['metadata'].get('signal_strength')
        )
    
    def aggregate_window_data(self):
        """Aggregate data within time windows."""
        session = self.Session()
        
        try:
            for key, readings in self.window_data.items():
                if not readings:
                    continue
                
                df = pd.DataFrame([{
                    'value': r.value,
                    'timestamp': r.timestamp
                } for r in readings])
                
                aggregated = AggregatedMetrics(
                    sensor_id=key[0],
                    sensor_type=key[1],
                    window_start=df['timestamp'].min(),
                    window_end=df['timestamp'].max(),
                    avg_value=df['value'].mean(),
                    min_value=df['value'].min(),
                    max_value=df['value'].max(),
                    count=len(df),
                    std_value=df['value'].std()
                )
                
                session.add(aggregated)
                
                logger.info("Aggregated metrics",
                           sensor_id=key[0],
                           sensor_type=key[1],
                           count=len(df),
                           avg=round(df['value'].mean(), 2))
            
            session.commit()
            self.window_data.clear()
            
        except Exception as e:
            session.rollback()
            logger.error("Failed to aggregate data", error=str(e))
        finally:
            session.close()
    
    def process_batch(self, readings: List[SensorReading]):
        """Process a batch of sensor readings."""
        session = self.Session()
        
        try:
            # Store raw readings
            session.bulk_save_objects(readings)
            session.commit()
            
            # Add to window for aggregation
            for reading in readings:
                key = (reading.sensor_id, reading.sensor_type)
                self.window_data[key].append(reading)
            
            # Check if window is complete
            if datetime.utcnow() - self.last_batch_time > self.window_size:
                self.aggregate_window_data()
                self.last_batch_time = datetime.utcnow()
            
            # Update metrics
            self.metrics.messages_processed.inc(len(readings))
            
            logger.info("Processed batch", batch_size=len(readings))
            
        except Exception as e:
            session.rollback()
            logger.error("Failed to process batch", error=str(e))
            self.metrics.errors.inc()
        finally:
            session.close()
    
    def consume(self):
        """Main consumer loop."""
        logger.info("Starting analytics consumer", topic=self.topic)
        
        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)
                
                if msg is None:
                    # Check if batch timeout reached
                    if self.message_batch and \
                       (datetime.utcnow() - self.last_batch_time).seconds > self.batch_timeout:
                        readings = [self.process_message(m) for m in self.message_batch]
                        self.process_batch(readings)
                        self.message_batch = []
                    continue
                
                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    else:
                        raise KafkaException(msg.error())
                
                # Parse message
                message = json.loads(msg.value().decode('utf-8'))
                self.message_batch.append(message)
                
                # Process batch if size reached
                if len(self.message_batch) >= self.batch_size:
                    readings = [self.process_message(m) for m in self.message_batch]
                    self.process_batch(readings)
                    self.message_batch = []
                    self.consumer.commit(asynchronous=False)
                
        except KeyboardInterrupt:
            logger.info("Shutting down consumer...")
        finally:
            self.consumer.close()


if __name__ == '__main__':
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    )
    
    consumer = AnalyticsConsumer()
    consumer.consume()
