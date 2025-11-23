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
	pip install -r requirements.txt
	cp .env.example .env
	docker-compose up -d
	sleep 10
	python utils/database.py

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
	pytest tests/ -v --cov=.

lint:
	flake8 . --max-line-length=100
	black --check .
	mypy .

producer:
	python producers/sensor_producer.py &
	python producers/ecommerce_producer.py &

consumer:
	python consumers/analytics_consumer.py &
	python consumers/alert_consumer.py &
