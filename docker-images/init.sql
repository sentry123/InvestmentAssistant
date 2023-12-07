-- init.sql

-- Create a database
CREATE DATABASE mydatabase;

-- Connect to the newly created database
\c mydatabase;

-- Enable the TimescaleDB extension
CREATE EXTENSION IF NOT EXISTS timescaledb;

-- Create a table with hypertable
-- (Add your TimescaleDB-specific configurations here)
CREATE TABLE crypto_price_data (
                             timestamp TIMESTAMPTZ NOT NULL,
                             symbol VARCHAR NOT NULL,
                             open   DOUBLE PRECISION NOT NULL,
                             high   DOUBLE PRECISION NOT NULL,
                             low    DOUBLE PRECISION NOT NULL,
                             close  DOUBLE PRECISION NOT NULL,
                             volume_crypto DOUBLE PRECISION NOT NULL,
                             volume_currency DOUBLE PRECISION NOT NULL,
                             weighted_price DOUBLE PRECISION NOT NULL
);

-- Create a hypertable based on the time column
SELECT create_hypertable('crypto_price_data', 'timestamp');
