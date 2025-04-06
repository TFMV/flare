# Flare

A high-performance Apache Arrow Flight gateway for Kafka that enables real-time analytics.

## Overview

Flare reads messages from Kafka topics, converts them to Arrow RecordBatches (zero-copy), and serves them as ephemeral Flight streams for real-time analytics tools such as DuckDB, Apache Iceberg, and PyArrow clients.

## Features

- Dynamic topic discovery
- Ephemeral streams with TTL/inactivity expiration
- Stream reusability for clients
- Efficient memory management
- Schema resolution via Schema Registry or inference
- Optional persistence to Iceberg

## Installation

```bash
pip install -e .
```

## Usage

### Start the Flight server

```bash
flare serve --source kafka:9092 --schema http://schema-registry:8081 --ttl 300
```

### Test client to fetch and print from a topic stream

```bash
flare test-client --topic my-topic
```

### Query using DuckDB

```python
import duckdb
import pyarrow.flight as flight

# Connect to Flare's Flight server
client = flight.FlightClient("grpc://localhost:8815")

# Query a Kafka topic as a table
conn = duckdb.connect()
conn.register_arrow_flight("my_topic", client, "my-topic")
result = conn.execute("SELECT * FROM my_topic LIMIT 10").fetchall()
print(result)
```

## Architecture

- **IngestLayer**: Consumes from Kafka topics and batches messages into Arrow RecordBatches
- **SchemaResolver**: Retrieves schema via Schema Registry or infers from messages
- **FlightStreamManager**: Maintains topic-to-stream mapping and handles stream lifecycles
- **FlareFlightServer**: Implements a PyArrow Flight server to serve data

## Requirements

- Python 3.10+
- Apache Arrow 12+
- Confluent Kafka Python client
