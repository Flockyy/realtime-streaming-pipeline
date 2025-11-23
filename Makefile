.PHONY: help setup start stop clean test lint

help:
	@echo "Available commands:"
	@echo "  make setup       - Install dependencies and setup environment"
	@echo "  make start       - Start all services"
	@echo "  make stop        - Stop all services"
	@echo "  make clean       - Clean up containers and volumes"
	@echo "  make test        - Run tests"
	@echo "  make lint        - Run linters"
	@echo "  make producer    - Start all producers"
	@echo "  make consumer    - Start all consumers"

setup:
	uv sync
	docker-compose up -d
	sleep 10
	uv run python utils/database.py

start:
	docker-compose up -d
	@echo "Services started. Access:"
	@echo "  - Kafka UI: http://localhost:8080"
	@echo "  - Grafana: http://localhost:3000 (admin/admin)"
	@echo "  - PostgreSQL: localhost:5432"

stop:
	docker-compose down

clean:
	docker-compose down -v
	find . -type d -name __pycache__ -exec rm -rf {} +
	find . -type f -name "*.pyc" -delete

test:
	uv run pytest tests/ -v --cov=.

lint:
	uv run flake8 . --max-line-length=100
	uv run black --check .
	uv run mypy . --ignore-missing-imports

format:
	uv run black .

producer:
	uv run python producers/sensor_producer.py &
	uv run python producers/ecommerce_producer.py &

consumer:
	uv run python consumers/analytics_consumer.py &
	uv run python consumers/alert_consumer.py &

ml:
	uv run python ml/anomaly_detector.py &

api:
	uv run python api/main.py &

dlq:
	uv run python utils/dlq_handler.py &

services:
	$(MAKE) producer
	$(MAKE) consumer
	$(MAKE) ml
	$(MAKE) api
	$(MAKE) dlq

load-test:
	uv run locust -f tests/load_test.py --host=http://localhost:8000

integration-test:
	uv run pytest tests/test_integration.py -v

logs:
	docker-compose logs -f

ps:
	docker-compose ps
