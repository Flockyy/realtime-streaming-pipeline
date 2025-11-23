"""
Load Testing with Locust
Performance and load testing for the streaming pipeline.
"""

from locust import HttpUser, task, between, events
import json
import random
from datetime import datetime


class StreamingAPIUser(HttpUser):
    """Simulate users interacting with the streaming API."""
    
    wait_time = between(1, 3)
    host = "http://localhost:8000"
    
    def on_start(self):
        """Called when a simulated user starts."""
        self.sensor_ids = [f"sensor_{i:03d}" for i in range(100)]
        self.sensor_types = ["temperature", "humidity", "pressure"]
    
    @task(3)
    def get_latest_readings(self):
        """Get latest sensor readings."""
        params = {
            "sensor_id": random.choice(self.sensor_ids),
            "limit": random.randint(5, 20)
        }
        
        with self.client.get("/api/v1/sensors/latest", 
                            params=params,
                            catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if "readings" in data:
                    response.success()
                else:
                    response.failure("Invalid response format")
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(2)
    def get_sensor_stats(self):
        """Get sensor statistics."""
        params = {
            "time_window_minutes": random.choice([15, 30, 60])
        }
        
        with self.client.get("/api/v1/sensors/stats",
                            params=params,
                            catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if "total_messages" in data:
                    response.success()
                else:
                    response.failure("Invalid stats response")
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(1)
    def get_anomalies(self):
        """Get anomalous readings."""
        params = {
            "limit": random.randint(10, 30),
            "sensor_type": random.choice(self.sensor_types)
        }
        
        with self.client.get("/api/v1/sensors/anomalies",
                            params=params,
                            catch_response=True) as response:
            if response.status_code == 200:
                response.success()
            else:
                response.failure(f"Status code: {response.status_code}")
    
    @task(1)
    def health_check(self):
        """Health check endpoint."""
        with self.client.get("/", catch_response=True) as response:
            if response.status_code == 200:
                data = response.json()
                if data.get("status") == "healthy":
                    response.success()
                else:
                    response.failure("Service not healthy")
            else:
                response.failure(f"Status code: {response.status_code}")


@events.test_start.add_listener
def on_test_start(environment, **kwargs):
    """Called when load test starts."""
    print("Load test starting...")


@events.test_stop.add_listener
def on_test_stop(environment, **kwargs):
    """Called when load test stops."""
    print(f"Load test finished.")
    print(f"Total requests: {environment.stats.total.num_requests}")
    print(f"Total failures: {environment.stats.total.num_failures}")
    print(f"Average response time: {environment.stats.total.avg_response_time:.2f}ms")
    print(f"RPS: {environment.stats.total.current_rps:.2f}")


# Run with: locust -f tests/load_test.py --host=http://localhost:8000
# Then open http://localhost:8089 to configure and start the test
