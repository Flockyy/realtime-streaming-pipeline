"""
Database models and utilities.
"""

from datetime import datetime

from sqlalchemy import Boolean, Column, DateTime, Float, Integer, String, create_engine
from sqlalchemy.ext.declarative import declarative_base

Base = declarative_base()


class SensorReading(Base):
    """Model for raw sensor readings."""

    __tablename__ = "sensor_readings"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor_id = Column(String(50), nullable=False, index=True)
    sensor_type = Column(String(20), nullable=False, index=True)
    value = Column(Float, nullable=False)
    timestamp = Column(DateTime, nullable=False, index=True)
    location = Column(String(50))
    anomaly = Column(Boolean, default=False)
    battery_level = Column(Integer)
    signal_strength = Column(Integer)
    created_at = Column(DateTime, default=datetime.utcnow)


class AggregatedMetrics(Base):
    """Model for aggregated sensor metrics."""

    __tablename__ = "aggregated_metrics"

    id = Column(Integer, primary_key=True, autoincrement=True)
    sensor_id = Column(String(50), nullable=False, index=True)
    sensor_type = Column(String(20), nullable=False, index=True)
    window_start = Column(DateTime, nullable=False, index=True)
    window_end = Column(DateTime, nullable=False)
    avg_value = Column(Float)
    min_value = Column(Float)
    max_value = Column(Float)
    count = Column(Integer)
    std_value = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


class EcommerceEvent(Base):
    """Model for e-commerce events."""

    __tablename__ = "ecommerce_events"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(50), unique=True, nullable=False)
    event_type = Column(String(30), nullable=False, index=True)
    user_id = Column(String(50), nullable=False, index=True)
    timestamp = Column(DateTime, nullable=False, index=True)
    session_id = Column(String(50))
    device = Column(String(20))
    product_id = Column(Integer)
    product_name = Column(String(200))
    quantity = Column(Integer)
    price = Column(Float)
    total = Column(Float)
    created_at = Column(DateTime, default=datetime.utcnow)


def create_tables(db_url: str):
    """Create all database tables."""
    engine = create_engine(db_url)
    Base.metadata.create_all(engine)
    return engine


if __name__ == "__main__":
    # Create tables
    db_url = "postgresql://streaming_user:streaming_pass@localhost:5432/streaming_db"
    engine = create_tables(db_url)
    print("Database tables created successfully!")
