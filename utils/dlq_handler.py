"""
Dead Letter Queue Handler
Handles failed messages with retry logic and error tracking.
"""

import json
import time
from datetime import datetime
from typing import Any

import structlog
import yaml
from confluent_kafka import Consumer, KafkaError, Producer
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logger = structlog.get_logger()


class DLQHandler:
    """Dead Letter Queue handler with retry logic."""

    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the DLQ handler."""
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.consumer = Consumer(
            {
                "bootstrap.servers": ",".join(self.config["kafka"]["bootstrap_servers"]),
                "group.id": "dlq-handler",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": False,
            }
        )

        self.producer = Producer(
            {
                "bootstrap.servers": ",".join(self.config["kafka"]["bootstrap_servers"]),
                "client.id": "dlq-handler",
            }
        )

        # Database for error tracking
        db_config = self.config["postgres"]
        DATABASE_URL = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
        self.engine = create_engine(DATABASE_URL, poolclass=NullPool)

        self.max_retries = 3
        self.retry_delay = 5  # seconds

        # Create DLQ topics
        self.dlq_topic = "dlq-messages"

        logger.info("DLQ handler initialized")

    def log_error(self, topic: str, message_data: dict[str, Any], error: str, retry_count: int):
        """Log error to database."""
        try:
            query = text(
                """
                INSERT INTO dlq_errors (topic, message_data, error, retry_count, created_at)
                VALUES (:topic, :message_data, :error, :retry_count, :created_at)
            """
            )

            with self.engine.connect() as conn:
                conn.execute(
                    query,
                    {
                        "topic": topic,
                        "message_data": json.dumps(message_data),
                        "error": error,
                        "retry_count": retry_count,
                        "created_at": datetime.utcnow(),
                    },
                )
                conn.commit()

        except Exception as e:
            logger.error("Failed to log error to database", error=str(e))

    def send_to_dlq(
        self, original_topic: str, message_data: dict[str, Any], error: str, retry_count: int
    ):
        """Send failed message to DLQ."""
        dlq_message = {
            "original_topic": original_topic,
            "message_data": message_data,
            "error": error,
            "retry_count": retry_count,
            "timestamp": datetime.utcnow().isoformat(),
            "final_failure": retry_count >= self.max_retries,
        }

        self.producer.produce(topic=self.dlq_topic, value=json.dumps(dlq_message))
        self.producer.flush()

        # Log to database
        self.log_error(original_topic, message_data, error, retry_count)

        logger.warning(
            "Message sent to DLQ",
            original_topic=original_topic,
            retry_count=retry_count,
            error=error[:100],
        )

    def retry_message(self, topic: str, message_data: dict[str, Any], retry_count: int) -> bool:
        """Attempt to retry processing a message."""
        try:
            # Implement custom retry logic here
            # For now, just re-send to original topic
            self.producer.produce(topic=topic, value=json.dumps(message_data))
            self.producer.flush()

            logger.info("Message retried successfully", topic=topic, retry_count=retry_count)
            return True

        except Exception as e:
            logger.error("Retry failed", topic=topic, retry_count=retry_count, error=str(e))
            return False

    def process_dlq_message(self, message: dict[str, Any]):
        """Process a message from the DLQ."""
        original_topic = message["original_topic"]
        message_data = message["message_data"]
        retry_count = message["retry_count"]
        error = message["error"]

        if retry_count < self.max_retries:
            # Wait before retry
            time.sleep(self.retry_delay * (retry_count + 1))

            # Attempt retry
            success = self.retry_message(original_topic, message_data, retry_count)

            if not success:
                # Increment retry count and send back to DLQ
                self.send_to_dlq(original_topic, message_data, error, retry_count + 1)
        else:
            logger.error(
                "Message exceeded max retries",
                original_topic=original_topic,
                retry_count=retry_count,
            )
            # Could send to a permanent failure topic or alert admins

    def run(self):
        """Run the DLQ handler."""
        self.consumer.subscribe([self.dlq_topic])
        logger.info("DLQ handler running")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    logger.error("Consumer error", error=str(msg.error()))
                    continue

                # Process DLQ message
                try:
                    dlq_message = json.loads(msg.value().decode("utf-8"))
                    self.process_dlq_message(dlq_message)
                    self.consumer.commit(msg)

                except Exception as e:
                    logger.error("Error processing DLQ message", error=str(e))

        except KeyboardInterrupt:
            logger.info("Shutting down DLQ handler...")

        finally:
            self.consumer.close()
            self.producer.flush()


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    handler = DLQHandler()
    handler.run()
