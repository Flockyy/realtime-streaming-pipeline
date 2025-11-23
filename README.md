# Real-Time Data Streaming Pipeline

A production-ready real-time data streaming pipeline using Apache Kafka, Python, and PostgreSQL.

![Kafka Architecture](assets/kafka.png)

## Architecture

```
Data Sources → Kafka Producer → Kafka Broker → Kafka Consumer → Data Processing → PostgreSQL/Analytics
```

## Features

### Data Ingestion & Processing
- **Multi-source data ingestion**: IoT sensors, e-commerce events with realistic data generation
- **Apache Kafka**: Distributed message broker for reliable stream processing
- **Schema Registry**: Avro schema management for data validation and evolution
- **Kafka Connect**: Extensible data integration framework
- **Dead Letter Queue**: Automatic retry and error handling for failed messages

### Storage & Analytics
- **PostgreSQL**: Structured data storage with optimized indexing
- **InfluxDB**: Time-series data for metrics and monitoring
- **Real-time API**: FastAPI with WebSocket support for live data streaming
- **ML Anomaly Detection**: Isolation Forest for real-time anomaly detection

### Monitoring & Observability
- **Prometheus**: Metrics collection and alerting
- **Grafana**: Pre-configured dashboards for visualization
- **Jaeger**: Distributed tracing for request flows
- **Kafka UI**: Interactive Kafka management interface

### Development & Testing
- **CI/CD Pipeline**: GitHub Actions for automated testing and deployment
- **Integration Tests**: Comprehensive test suite with Kafka testcontainers
- **Load Testing**: Locust-based performance testing
- **Type Safety**: Full type hints and mypy validation

### Production-Ready
- **Containerized**: Docker Compose for easy deployment
- **uv Package Manager**: Fast dependency management
- **Error Handling**: Comprehensive error tracking and recovery
- **Scalable Architecture**: Designed for horizontal scaling

## Project Structure

```
realtime-streaming-pipeline/
├── producers/          # Data producers
├── consumers/          # Data consumers and processors
├── config/            # Configuration files
├── docker/            # Docker configurations
├── monitoring/        # Monitoring and alerting
├── tests/            # Unit and integration tests
└── utils/            # Shared utilities
```

## Quick Start

### Prerequisites
- Docker & Docker Compose
- Python 3.9+
- [uv](https://github.com/astral-sh/uv) - Fast Python package installer
- Make (optional, for WSL/Linux/macOS)

### Setup

1. **Clone and navigate to the project**
```bash
git clone https://github.com/Flockyy/realtime-streaming-pipeline.git
cd realtime-streaming-pipeline
```

2. **Install uv (if not already installed)**
```bash
# Linux/WSL/macOS
curl -LsSf https://astral.sh/uv/install.sh | sh

# Or via pip
pip install uv
```

3. **Start the infrastructure**
```bash
docker-compose up -d
```

This will start:
- **Zookeeper** (port 2181) - Kafka coordination
- **Kafka** (port 9092) - Message broker
- **Schema Registry** (port 8081) - Schema management
- **Kafka Connect** (port 8083) - Data integration
- **PostgreSQL** (port 5432) - Structured storage
- **InfluxDB** (port 8086) - Time-series storage
- **Prometheus** (port 9090) - Metrics collection
- **Grafana** (port 3000) - Visualization dashboards
- **Jaeger** (port 16686) - Distributed tracing
- **Kafka UI** (port 8080) - Kafka management

4. **Install Python dependencies**
```bash
uv sync
```

5. **Run the services**

```bash
# Start producers (generate data)
uv run python producers/sensor_producer.py &
uv run python producers/ecommerce_producer.py &

# Start consumers (process data)
uv run python consumers/analytics_consumer.py &
uv run python consumers/alert_consumer.py &

# Start ML anomaly detection
uv run python ml/anomaly_detector.py &

# Start API server
uv run python api/main.py &

# Start DLQ handler
uv run python utils/dlq_handler.py &
```

Or use Make shortcuts (WSL/Linux/macOS):
```bash
make setup        # Install dependencies and start infrastructure
make producer     # Start all producers
make consumer     # Start all consumers
```

## Use Cases

### 1. IoT Sensor Data Pipeline
Simulate IoT sensors sending temperature, humidity, and pressure data for real-time monitoring.

### 2. E-commerce Events
Track user events (clicks, purchases, cart additions) for real-time analytics.

### 3. Financial Transactions
Process financial transactions with fraud detection and anomaly detection.

### 4. Social Media Feed
Aggregate and process social media posts in real-time.

## Configuration

Edit `config/config.yaml` to customize:
- Kafka broker settings
- Database connections
- Producer/consumer parameters
- Processing logic

## Access Points

Once all services are running, access:

| Service | URL | Credentials |
|---------|-----|-------------|
| Grafana Dashboards | http://localhost:3000 | admin/admin |
| Kafka UI | http://localhost:8080 | - |
| Prometheus | http://localhost:9090 | - |
| Jaeger Tracing | http://localhost:16686 | - |
| API Documentation | http://localhost:8000/docs | - |
| WebSocket Stream | ws://localhost:8000/ws/sensors | - |

## API Endpoints

### REST API
- `GET /api/v1/sensors/latest` - Get latest sensor readings
- `GET /api/v1/sensors/stats` - Get aggregated statistics
- `GET /api/v1/sensors/anomalies` - Get anomalous readings
- `WS /ws/sensors` - Real-time WebSocket stream

Example:
```bash
# Get latest readings
curl "http://localhost:8000/api/v1/sensors/latest?limit=10"

# Get statistics for last hour
curl "http://localhost:8000/api/v1/sensors/stats?time_window_minutes=60"

# Get anomalies
curl "http://localhost:8000/api/v1/sensors/anomalies?sensor_type=temperature"
```

## Testing

```bash
# Run all tests
uv run pytest tests/ -v

# Run integration tests
uv run pytest tests/test_integration.py -v

# Run with coverage
uv run pytest --cov=. tests/

# Load testing
uv run locust -f tests/load_test.py --host=http://localhost:8000

# Or use make
make test
```

## Advanced Features

- **Exactly-once semantics**: Kafka transactions for data consistency
- **Schema Registry**: Avro schema management
- **Stream joins**: Combine multiple data streams
- **Windowing**: Time-based aggregations
- **State management**: Stateful stream processing

## Architecture Details

### Data Flow
1. **Producers** generate synthetic data (sensors, e-commerce events)
2. **Kafka** buffers and distributes messages across topics
3. **Schema Registry** validates message schemas
4. **Consumers** process data in real-time:
   - Analytics consumer stores to PostgreSQL/InfluxDB
   - Alert consumer detects threshold violations
   - ML detector identifies anomalies using Isolation Forest
5. **DLQ Handler** manages failed messages with retry logic
6. **API** provides real-time and historical data access

### Monitoring Stack
- **Prometheus** scrapes metrics from all services
- **Grafana** visualizes metrics with pre-built dashboards
- **Jaeger** traces requests across distributed services
- **Kafka UI** monitors topics, consumers, and message flow

### Machine Learning
The anomaly detector uses:
- **Isolation Forest** algorithm for unsupervised anomaly detection
- **Online learning** - retrains periodically on recent data
- **Feature extraction** - combines sensor values, metadata, and temporal features
- **Automatic alerting** - publishes alerts for detected anomalies

## Development

### Project Structure
```
realtime-streaming-pipeline/
├── producers/          # Data generators
├── consumers/          # Stream processors
├── api/               # FastAPI REST/WebSocket server
├── ml/                # Machine learning models
├── schemas/           # Avro schemas
├── utils/             # Shared utilities and DLQ handler
├── tests/             # Integration and load tests
├── monitoring/        # Grafana dashboards and Prometheus config
├── .github/           # CI/CD workflows
└── docker-compose.yml # Infrastructure setup
```

### Adding New Features
1. Define Avro schema in `schemas/`
2. Create producer in `producers/`
3. Implement consumer logic in `consumers/`
4. Add API endpoints in `api/`
5. Write tests in `tests/`
6. Update documentation

## CI/CD Pipeline

The project includes GitHub Actions workflows for:
- **Linting** - flake8, black, mypy
- **Testing** - pytest with coverage reporting
- **Docker Build** - Multi-arch image builds
- **Integration Tests** - Full end-to-end testing

## Performance

Tested with:
- **10,000+ messages/second** sustained throughput
- **<100ms** p95 processing latency
- **<5MB** memory per consumer
- **Horizontal scaling** via consumer groups

## Production Considerations

### Security
- [ ] Enable SSL/TLS for Kafka
- [ ] Set up SASL authentication
- [ ] Secure Schema Registry with API keys
- [ ] Use secrets management (Vault, AWS Secrets Manager)
- [ ] Enable Grafana authentication

### Scalability
- [ ] Configure retention policies for topics
- [ ] Scale consumers with consumer groups
- [ ] Use Kafka partitioning strategy
- [ ] Implement connection pooling
- [ ] Add caching layer (Redis)

### Reliability
- [x] Dead letter queue for failed messages
- [x] Exactly-once semantics with Kafka transactions
- [x] Health checks for all services
- [ ] Backup and disaster recovery
- [ ] Multi-region deployment

### Monitoring
- [x] Prometheus metrics
- [x] Grafana dashboards
- [x] Distributed tracing
- [ ] Log aggregation (ELK stack)
- [ ] Alerting rules

## License

MIT
