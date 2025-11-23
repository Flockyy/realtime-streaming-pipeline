"""
Real-time Streaming API
FastAPI application for querying streaming data and WebSocket updates.
"""

import asyncio
import json
from contextlib import asynccontextmanager
from datetime import datetime
from typing import Optional

import structlog
import yaml
from confluent_kafka import Consumer, KafkaError
from fastapi import FastAPI, HTTPException, Query, WebSocket, WebSocketDisconnect
from fastapi.middleware.cors import CORSMiddleware
from pydantic import BaseModel
from sqlalchemy import create_engine, text
from sqlalchemy.pool import NullPool

logger = structlog.get_logger()


# Models
class SensorQuery(BaseModel):
    sensor_ids: Optional[list[str]] = None
    sensor_types: Optional[list[str]] = None
    start_time: Optional[datetime] = None
    end_time: Optional[datetime] = None
    limit: int = 100


class StatsResponse(BaseModel):
    total_messages: int
    messages_per_second: float
    active_sensors: int
    anomaly_count: int
    avg_value: float


# Connection manager for WebSockets
class ConnectionManager:
    def __init__(self):
        self.active_connections: list[WebSocket] = []

    async def connect(self, websocket: WebSocket):
        await websocket.accept()
        self.active_connections.append(websocket)
        logger.info("WebSocket connected", total_connections=len(self.active_connections))

    def disconnect(self, websocket: WebSocket):
        self.active_connections.remove(websocket)
        logger.info("WebSocket disconnected", total_connections=len(self.active_connections))

    async def broadcast(self, message: dict):
        for connection in self.active_connections:
            try:
                await connection.send_json(message)
            except Exception as e:
                logger.error("Error broadcasting message", error=str(e))


manager = ConnectionManager()


# Load configuration
with open("config/config.yaml") as f:
    config = yaml.safe_load(f)

# Database connection
db_config = config["postgres"]
DATABASE_URL = f"postgresql://{db_config['user']}:{db_config['password']}@{db_config['host']}:{db_config['port']}/{db_config['database']}"
engine = create_engine(DATABASE_URL, poolclass=NullPool)


# Kafka consumer for real-time streaming
consumer = None


async def kafka_consumer_task():
    """Background task to consume Kafka messages and broadcast via WebSocket."""
    global consumer

    consumer = Consumer(
        {
            "bootstrap.servers": ",".join(config["kafka"]["bootstrap_servers"]),
            "group.id": "api-websocket-consumer",
            "auto.offset.reset": "latest",
            "enable.auto.commit": True,
        }
    )

    consumer.subscribe([config["kafka"]["topics"]["sensor_data"]])

    logger.info("Started Kafka consumer for WebSocket broadcasting")

    while True:
        try:
            msg = consumer.poll(timeout=1.0)

            if msg is None:
                await asyncio.sleep(0.1)
                continue

            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    logger.error("Kafka error", error=str(msg.error()))
                    continue

            # Parse and broadcast message
            data = json.loads(msg.value().decode("utf-8"))
            await manager.broadcast(
                {
                    "type": "sensor_reading",
                    "data": data,
                    "received_at": datetime.utcnow().isoformat(),
                }
            )

        except Exception as e:
            logger.error("Error in Kafka consumer", error=str(e))
            await asyncio.sleep(1)


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Startup
    logger.info("Starting API server")
    consumer_task = asyncio.create_task(kafka_consumer_task())
    yield
    # Shutdown
    logger.info("Shutting down API server")
    consumer_task.cancel()
    if consumer:
        consumer.close()


# FastAPI app
app = FastAPI(
    title="Real-time Streaming API",
    description="API for querying and streaming real-time data",
    version="1.0.0",
    lifespan=lifespan,
)

# CORS middleware
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)


@app.get("/")
async def root():
    """Health check endpoint."""
    return {
        "status": "healthy",
        "service": "streaming-api",
        "timestamp": datetime.utcnow().isoformat(),
    }


@app.get("/api/v1/sensors/latest")
async def get_latest_readings(
    sensor_id: Optional[str] = None,
    sensor_type: Optional[str] = None,
    limit: int = Query(default=10, le=100),
):
    """Get latest sensor readings from database."""
    try:
        query = "SELECT * FROM sensor_readings WHERE 1=1"
        params = {}

        if sensor_id:
            query += " AND sensor_id = :sensor_id"
            params["sensor_id"] = sensor_id

        if sensor_type:
            query += " AND sensor_type = :sensor_type"
            params["sensor_type"] = sensor_type

        query += " ORDER BY timestamp DESC LIMIT :limit"
        params["limit"] = limit

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            readings = [dict(row._mapping) for row in result]

        return {"count": len(readings), "readings": readings}

    except Exception as e:
        logger.error("Error fetching readings", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sensors/stats")
async def get_sensor_stats(
    time_window_minutes: int = Query(default=60, ge=1, le=1440),
) -> StatsResponse:
    """Get aggregated statistics for sensors."""
    try:
        query = """
        SELECT
            COUNT(*) as total_messages,
            COUNT(DISTINCT sensor_id) as active_sensors,
            SUM(CASE WHEN anomaly = true THEN 1 ELSE 0 END) as anomaly_count,
            AVG(value) as avg_value
        FROM sensor_readings
        WHERE timestamp >= NOW() - INTERVAL ':minutes minutes'
        """

        with engine.connect() as conn:
            result = conn.execute(text(query), {"minutes": time_window_minutes}).fetchone()

        if result:
            messages_per_second = result[0] / (time_window_minutes * 60)
            return StatsResponse(
                total_messages=result[0] or 0,
                messages_per_second=round(messages_per_second, 2),
                active_sensors=result[1] or 0,
                anomaly_count=result[2] or 0,
                avg_value=round(float(result[3] or 0), 2),
            )

        return StatsResponse(
            total_messages=0,
            messages_per_second=0.0,
            active_sensors=0,
            anomaly_count=0,
            avg_value=0.0,
        )

    except Exception as e:
        logger.error("Error fetching stats", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.get("/api/v1/sensors/anomalies")
async def get_anomalies(limit: int = Query(default=20, le=100), sensor_type: Optional[str] = None):
    """Get recent anomalous readings."""
    try:
        query = "SELECT * FROM sensor_readings WHERE anomaly = true"
        params = {}

        if sensor_type:
            query += " AND sensor_type = :sensor_type"
            params["sensor_type"] = sensor_type

        query += " ORDER BY timestamp DESC LIMIT :limit"
        params["limit"] = limit

        with engine.connect() as conn:
            result = conn.execute(text(query), params)
            anomalies = [dict(row._mapping) for row in result]

        return {"count": len(anomalies), "anomalies": anomalies}

    except Exception as e:
        logger.error("Error fetching anomalies", error=str(e))
        raise HTTPException(status_code=500, detail=str(e))


@app.websocket("/ws/sensors")
async def websocket_endpoint(websocket: WebSocket):
    """WebSocket endpoint for real-time sensor data streaming."""
    await manager.connect(websocket)
    try:
        while True:
            # Keep connection alive and handle client messages
            await websocket.receive_text()
            # Echo back acknowledgment
            await websocket.send_json(
                {"type": "ack", "message": "connected", "timestamp": datetime.utcnow().isoformat()}
            )
    except WebSocketDisconnect:
        manager.disconnect(websocket)
    except Exception as e:
        logger.error("WebSocket error", error=str(e))
        manager.disconnect(websocket)


if __name__ == "__main__":
    import uvicorn

    structlog.configure(
        processors=[
            structlog.processors.TimeStamper(fmt="iso"),
            structlog.processors.JSONRenderer(),
        ]
    )

    uvicorn.run(app, host="0.0.0.0", port=8000)
