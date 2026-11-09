-- Dimensiones
CREATE TABLE IF NOT EXISTS dim_country (
    country_id SERIAL PRIMARY KEY,
    country_name VARCHAR(100) UNIQUE
);

CREATE TABLE IF NOT EXISTS dim_date (
    date_id SERIAL PRIMARY KEY,
    year INT UNIQUE
);

-- Tabla Raw (Staging)
CREATE TABLE IF NOT EXISTS raw_happiness_events (
    raw_event_id VARCHAR(50) PRIMARY KEY,
    event_payload TEXT,
    processing_status VARCHAR(50),
    received_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Tabla de Hechos (Fact Table)
CREATE TABLE IF NOT EXISTS fact_predictions (
    prediction_id SERIAL PRIMARY KEY,
    raw_event_id VARCHAR(50) REFERENCES raw_happiness_events(raw_event_id),
    country_id INT REFERENCES dim_country(country_id),
    date_id INT REFERENCES dim_date(date_id),
    actual_score FLOAT,
    predicted_score FLOAT,
    prediction_error FLOAT,
    event_time TIMESTAMP,
    prediction_timestamp TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);