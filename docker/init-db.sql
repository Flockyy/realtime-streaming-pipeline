-- Initialize streaming database

-- Create sensor_readings table
CREATE TABLE IF NOT EXISTS sensor_readings (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    value FLOAT NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    location VARCHAR(50),
    anomaly BOOLEAN DEFAULT FALSE,
    battery_level INTEGER,
    signal_strength INTEGER,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_sensor_readings_sensor_id ON sensor_readings(sensor_id);
CREATE INDEX idx_sensor_readings_timestamp ON sensor_readings(timestamp);
CREATE INDEX idx_sensor_readings_sensor_type ON sensor_readings(sensor_type);

-- Create aggregated_metrics table
CREATE TABLE IF NOT EXISTS aggregated_metrics (
    id SERIAL PRIMARY KEY,
    sensor_id VARCHAR(50) NOT NULL,
    sensor_type VARCHAR(20) NOT NULL,
    window_start TIMESTAMP NOT NULL,
    window_end TIMESTAMP NOT NULL,
    avg_value FLOAT,
    min_value FLOAT,
    max_value FLOAT,
    count INTEGER,
    std_value FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_aggregated_metrics_sensor_id ON aggregated_metrics(sensor_id);
CREATE INDEX idx_aggregated_metrics_window_start ON aggregated_metrics(window_start);

-- Create ecommerce_events table
CREATE TABLE IF NOT EXISTS ecommerce_events (
    id SERIAL PRIMARY KEY,
    event_id VARCHAR(50) UNIQUE NOT NULL,
    event_type VARCHAR(30) NOT NULL,
    user_id VARCHAR(50) NOT NULL,
    timestamp TIMESTAMP NOT NULL,
    session_id VARCHAR(50),
    device VARCHAR(20),
    product_id INTEGER,
    product_name VARCHAR(200),
    quantity INTEGER,
    price FLOAT,
    total FLOAT,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_ecommerce_events_user_id ON ecommerce_events(user_id);
CREATE INDEX idx_ecommerce_events_timestamp ON ecommerce_events(timestamp);
CREATE INDEX idx_ecommerce_events_event_type ON ecommerce_events(event_type);

-- Create materialized view for real-time analytics
CREATE MATERIALIZED VIEW IF NOT EXISTS sensor_stats AS
SELECT
    sensor_id,
    sensor_type,
    COUNT(*) as total_readings,
    AVG(value) as avg_value,
    MIN(value) as min_value,
    MAX(value) as max_value,
    MAX(timestamp) as last_reading
FROM sensor_readings
GROUP BY sensor_id, sensor_type;

CREATE UNIQUE INDEX idx_sensor_stats ON sensor_stats(sensor_id, sensor_type);

-- Grant permissions
GRANT ALL PRIVILEGES ON ALL TABLES IN SCHEMA public TO streaming_user;
GRANT ALL PRIVILEGES ON ALL SEQUENCES IN SCHEMA public TO streaming_user;
