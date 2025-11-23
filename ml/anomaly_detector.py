"""
Real-time Anomaly Detection
Machine Learning module for detecting anomalies in sensor data.
"""

import json
from datetime import datetime
from typing import Any

import joblib
import numpy as np
import structlog
import yaml
from confluent_kafka import Consumer, Producer
from sklearn.ensemble import IsolationForest
from sklearn.preprocessing import StandardScaler

logger = structlog.get_logger()


class AnomalyDetector:
    """Real-time anomaly detection using Isolation Forest."""

    def __init__(self, config_path: str = "config/config.yaml"):
        """Initialize the anomaly detector."""
        with open(config_path) as f:
            self.config = yaml.safe_load(f)

        self.consumer = Consumer(
            {
                "bootstrap.servers": ",".join(self.config["kafka"]["bootstrap_servers"]),
                "group.id": "ml-anomaly-detector",
                "auto.offset.reset": "earliest",
                "enable.auto.commit": True,
            }
        )

        self.producer = Producer(
            {
                "bootstrap.servers": ",".join(self.config["kafka"]["bootstrap_servers"]),
                "client.id": "anomaly-detector",
            }
        )

        self.consumer.subscribe([self.config["kafka"]["topics"]["sensor_data"]])

        # Initialize models for each sensor type
        self.models = {}
        self.scalers = {}
        self.buffers = {}  # Store recent data for training

        for sensor_type in ["temperature", "humidity", "pressure"]:
            self.models[sensor_type] = IsolationForest(
                contamination=0.05, random_state=42, n_estimators=100
            )
            self.scalers[sensor_type] = StandardScaler()
            self.buffers[sensor_type] = []

        self.buffer_size = 1000
        self.training_interval = 100  # Retrain every N messages
        self.message_count = 0

        logger.info("Anomaly detector initialized")

    def extract_features(self, data: dict[str, Any]) -> np.ndarray:
        """Extract features from sensor reading."""
        features = [
            data["value"],
            data["metadata"]["battery_level"],
            data["metadata"]["signal_strength"],
            # Add time-based features
            datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).hour,
            datetime.fromisoformat(data["timestamp"].replace("Z", "+00:00")).minute,
        ]
        return np.array(features).reshape(1, -1)

    def train_model(self, sensor_type: str):
        """Train or retrain the model for a sensor type."""
        if len(self.buffers[sensor_type]) < 100:
            return

        logger.info(
            "Training model", sensor_type=sensor_type, buffer_size=len(self.buffers[sensor_type])
        )

        # Prepare training data
        X = np.vstack([self.extract_features(d) for d in self.buffers[sensor_type]])

        # Fit scaler and transform data
        X_scaled = self.scalers[sensor_type].fit_transform(X)

        # Train model
        self.models[sensor_type].fit(X_scaled)

        logger.info("Model trained", sensor_type=sensor_type)

    def detect_anomaly(self, data: dict[str, Any]) -> tuple[bool, float]:
        """
        Detect if a reading is anomalous.
        Returns: (is_anomaly, anomaly_score)
        """
        sensor_type = data["sensor_type"]

        # Extract features
        features = self.extract_features(data)

        # Check if model is trained
        try:
            # Scale features
            features_scaled = self.scalers[sensor_type].transform(features)

            # Predict
            prediction = self.models[sensor_type].predict(features_scaled)[0]
            anomaly_score = self.models[sensor_type].score_samples(features_scaled)[0]

            is_anomaly = prediction == -1

            return is_anomaly, float(anomaly_score)

        except Exception as e:
            # Model not trained yet, use simple threshold
            logger.debug("Model not trained, using fallback", sensor_type=sensor_type, error=str(e))
            return False, 0.0

    def process_message(self, data: dict[str, Any]):
        """Process a sensor reading and detect anomalies."""
        sensor_type = data["sensor_type"]

        # Add to buffer
        self.buffers[sensor_type].append(data)

        # Keep buffer size limited
        if len(self.buffers[sensor_type]) > self.buffer_size:
            self.buffers[sensor_type] = self.buffers[sensor_type][-self.buffer_size :]

        # Retrain periodically
        self.message_count += 1
        if self.message_count % self.training_interval == 0:
            self.train_model(sensor_type)

        # Detect anomaly
        is_anomaly, anomaly_score = self.detect_anomaly(data)

        # If anomaly detected, publish to alerts topic
        if is_anomaly:
            alert = {
                "alert_type": "ml_anomaly",
                "sensor_id": data["sensor_id"],
                "sensor_type": sensor_type,
                "value": data["value"],
                "anomaly_score": anomaly_score,
                "timestamp": data["timestamp"],
                "location": data["location"],
                "severity": "high" if anomaly_score < -0.5 else "medium",
                "message": f"ML-detected anomaly in {sensor_type} sensor {data['sensor_id']}",
            }

            self.producer.produce(
                topic=self.config["kafka"]["topics"]["alerts"],
                key=data["sensor_id"],
                value=json.dumps(alert),
            )

            logger.warning(
                "Anomaly detected",
                sensor_id=data["sensor_id"],
                sensor_type=sensor_type,
                anomaly_score=anomaly_score,
            )

    def run(self):
        """Run the anomaly detection service."""
        logger.info("Starting anomaly detection service")

        try:
            while True:
                msg = self.consumer.poll(timeout=1.0)

                if msg is None:
                    continue

                if msg.error():
                    logger.error("Consumer error", error=str(msg.error()))
                    continue

                # Parse message
                data = json.loads(msg.value().decode("utf-8"))

                # Process message
                self.process_message(data)

                # Flush producer
                self.producer.poll(0)

        except KeyboardInterrupt:
            logger.info("Shutting down anomaly detector...")

        finally:
            self.consumer.close()
            self.producer.flush()

    def save_models(self, path: str = "ml/models"):
        """Save trained models to disk."""
        import os

        os.makedirs(path, exist_ok=True)

        for sensor_type in self.models.keys():
            model_path = f"{path}/model_{sensor_type}.joblib"
            scaler_path = f"{path}/scaler_{sensor_type}.joblib"

            joblib.dump(self.models[sensor_type], model_path)
            joblib.dump(self.scalers[sensor_type], scaler_path)

        logger.info("Models saved", path=path)

    def load_models(self, path: str = "ml/models"):
        """Load trained models from disk."""
        import os

        for sensor_type in self.models.keys():
            model_path = f"{path}/model_{sensor_type}.joblib"
            scaler_path = f"{path}/scaler_{sensor_type}.joblib"

            if os.path.exists(model_path) and os.path.exists(scaler_path):
                self.models[sensor_type] = joblib.load(model_path)
                self.scalers[sensor_type] = joblib.load(scaler_path)
                logger.info("Model loaded", sensor_type=sensor_type)


if __name__ == "__main__":
    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    detector = AnomalyDetector()
    detector.run()
