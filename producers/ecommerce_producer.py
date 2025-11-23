"""
E-commerce Events Producer
Simulates user interactions on an e-commerce platform.
"""

import json
import time
import random
from datetime import datetime
from typing import Dict, Any
from confluent_kafka import Producer
import yaml
import structlog
from faker import Faker

logger = structlog.get_logger()
fake = Faker()


class EcommerceProducer:
    def __init__(self, config_path: str = 'config/config.yaml'):
        """Initialize the e-commerce producer."""
        with open(config_path, 'r') as f:
            self.config = yaml.safe_load(f)
        
        self.producer = Producer({
            'bootstrap.servers': ','.join(self.config['kafka']['bootstrap_servers']),
            'client.id': 'ecommerce-producer'
        })
        
        self.topic = self.config['kafka']['topics']['ecommerce_events']
        self.interval = self.config['producers']['ecommerce']['interval_seconds']
        
        # Sample data
        self.products = [
            {'id': i, 'name': fake.catch_phrase(), 'price': round(random.uniform(10, 500), 2)}
            for i in range(100)
        ]
        
        self.users = [f'user_{i:05d}' for i in range(1000)]
    
    def delivery_report(self, err, msg):
        """Callback for message delivery reports."""
        if err is not None:
            logger.error('Message delivery failed', error=str(err))
        else:
            logger.debug('Message delivered', topic=msg.topic())
    
    def generate_event(self) -> Dict[str, Any]:
        """Generate an e-commerce event."""
        event_type = random.choice(self.config['producers']['ecommerce']['events'])
        user_id = random.choice(self.users)
        product = random.choice(self.products)
        
        base_event = {
            'event_id': fake.uuid4(),
            'event_type': event_type,
            'user_id': user_id,
            'timestamp': datetime.utcnow().isoformat(),
            'session_id': fake.uuid4()[:8],
            'device': random.choice(['mobile', 'desktop', 'tablet']),
            'ip_address': fake.ipv4()
        }
        
        if event_type == 'page_view':
            base_event.update({
                'page': random.choice(['home', 'product', 'category', 'cart', 'checkout']),
                'duration_seconds': random.randint(5, 300)
            })
        
        elif event_type in ['add_to_cart', 'purchase']:
            quantity = random.randint(1, 5)
            base_event.update({
                'product_id': product['id'],
                'product_name': product['name'],
                'quantity': quantity,
                'price': product['price'],
                'total': round(product['price'] * quantity, 2)
            })
            
            if event_type == 'purchase':
                base_event.update({
                    'payment_method': random.choice(['credit_card', 'paypal', 'debit_card']),
                    'shipping_address': fake.address().replace('\n', ', ')
                })
        
        elif event_type == 'search':
            base_event.update({
                'query': fake.catch_phrase(),
                'results_count': random.randint(0, 100)
            })
        
        return base_event
    
    def produce(self):
        """Continuously produce e-commerce events."""
        logger.info("Starting e-commerce producer", interval=self.interval)
        
        try:
            while True:
                event = self.generate_event()
                
                self.producer.produce(
                    topic=self.topic,
                    key=event['user_id'],
                    value=json.dumps(event),
                    callback=self.delivery_report
                )
                
                self.producer.poll(0)
                time.sleep(self.interval)
                
        except KeyboardInterrupt:
            logger.info("Shutting down producer...")
        finally:
            self.producer.flush()


if __name__ == '__main__':
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer()
        ]
    )
    
    producer = EcommerceProducer()
    producer.produce()
