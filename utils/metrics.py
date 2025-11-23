"""
Prometheus metrics for monitoring.
"""

from prometheus_client import Counter, Histogram, Gauge, start_http_server
import structlog

logger = structlog.get_logger()


class MetricsCollector:
    """Collects and exposes Prometheus metrics."""
    
    def __init__(self, port: int = 8000):
        # Counters
        self.messages_produced = Counter(
            'messages_produced_total',
            'Total number of messages produced',
            ['topic']
        )
        
        self.messages_processed = Counter(
            'messages_processed_total',
            'Total number of messages processed'
        )
        
        self.errors = Counter(
            'processing_errors_total',
            'Total number of processing errors'
        )
        
        # Histograms
        self.processing_duration = Histogram(
            'processing_duration_seconds',
            'Time spent processing messages',
            buckets=[0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1.0]
        )
        
        self.batch_size = Histogram(
            'batch_size',
            'Size of processed batches',
            buckets=[10, 25, 50, 100, 250, 500, 1000]
        )
        
        # Gauges
        self.consumer_lag = Gauge(
            'consumer_lag',
            'Consumer lag per partition',
            ['topic', 'partition']
        )
        
        self.active_sensors = Gauge(
            'active_sensors',
            'Number of active sensors'
        )
        
        # Start metrics server
        try:
            start_http_server(port)
            logger.info(f"Metrics server started on port {port}")
        except Exception as e:
            logger.warning(f"Could not start metrics server: {e}")


if __name__ == '__main__':
    # Test metrics
    metrics = MetricsCollector(port=8001)
    metrics.messages_processed.inc()
    print("Metrics available at http://localhost:8001")
