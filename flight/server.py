from typing import Dict, List, Optional, Any, Callable
import logging
import threading
import time
import json
from concurrent import futures
import pyarrow as pa
import pyarrow.flight as flight
from prometheus_client import Counter, Gauge, start_http_server

from flare.flight.stream_manager import FlightStreamManager
from flare.ingest.kafka_consumer import KafkaIngestLayer
from flare.schema.resolver import SchemaResolver, SchemaFormat

logger = logging.getLogger(__name__)

# Prometheus metrics
MESSAGES_PROCESSED = Counter(
    "flare_messages_processed_total", "Number of messages processed", ["topic"]
)
ACTIVE_STREAMS = Gauge("flare_active_streams", "Number of active streams")
RECORDS_SERVED = Counter(
    "flare_records_served_total", "Number of records served to clients", ["topic"]
)


class FlareFlightServer(flight.FlightServerBase):
    """
    PyArrow Flight server that serves Kafka messages as Arrow record batches.
    """

    def __init__(
        self,
        location: str = "grpc://0.0.0.0:8815",
        kafka_bootstrap_servers: str = "localhost:9092",
        schema_registry_url: Optional[str] = None,
        default_schema_format: SchemaFormat = SchemaFormat.INFER,
        topics: Optional[List[str]] = None,
        ttl: int = 300,
        batch_size: int = 1000,
        enable_metrics: bool = True,
        metrics_port: int = 8816,
    ):
        """
        Initialize the Flight server.

        Args:
            location: Flight server location string
            kafka_bootstrap_servers: Kafka bootstrap servers
            schema_registry_url: Schema Registry URL (optional)
            default_schema_format: Default schema format
            topics: List of Kafka topics to consume (None for auto-discovery)
            ttl: Time-to-live for streams in seconds
            batch_size: Maximum number of messages per Arrow batch
            enable_metrics: Whether to enable Prometheus metrics
            metrics_port: Port for Prometheus metrics HTTP server
        """
        super().__init__(location)
        self.location = location
        self.kafka_bootstrap_servers = kafka_bootstrap_servers
        self.schema_registry_url = schema_registry_url
        self.default_schema_format = default_schema_format
        self.topics = topics
        self.ttl = ttl
        self.batch_size = batch_size
        self.enable_metrics = enable_metrics
        self.metrics_port = metrics_port

        # Create stream manager
        self.stream_manager = FlightStreamManager(
            default_ttl=ttl,
            max_batches_per_stream=100,  # Keep up to 100 recent batches per topic
            cleanup_interval=60,  # Clean up expired streams every minute
        )

        # Create schema resolver
        self.schema_resolver = SchemaResolver(
            schema_registry_url=schema_registry_url,
            default_format=default_schema_format,
            cache_schemas=True,
        )

        # Create Kafka consumer for ingestion
        self.ingest_layer = KafkaIngestLayer(
            bootstrap_servers=kafka_bootstrap_servers,
            topics=topics,
            group_id="flare-consumer",
            batch_size=batch_size,
            schema_callback=self._get_schema_for_topic,
        )

        # Register batch callback
        for topic in topics or []:
            self.ingest_layer.register_batch_callback(topic, self._process_batch)

        # Start Prometheus metrics server if enabled
        if enable_metrics:
            try:
                start_http_server(metrics_port)
                logger.info(f"Started Prometheus metrics server on port {metrics_port}")
            except Exception as e:
                logger.error(f"Failed to start metrics server: {e}")

    def start(self):
        """Start the Flight server and Kafka consumer."""
        # Start Kafka consumer
        self.ingest_layer.start()
        logger.info(f"Started Kafka consumer for {self.kafka_bootstrap_servers}")

        # Register dynamic topics if needed
        if not self.topics:
            # TODO: Add dynamic topic discovery and registration
            pass

    def stop(self):
        """Stop the Flight server and Kafka consumer."""
        self.ingest_layer.stop()
        self.stream_manager.stop()
        logger.info("Stopped Flare server")

    def _get_schema_for_topic(self, topic: str) -> pa.Schema:
        """Get schema for a topic from the schema resolver."""
        try:
            # Try to get schema from registry or inference
            return self.schema_resolver.get_schema(topic)
        except Exception as e:
            logger.error(f"Failed to get schema for topic {topic}: {e}")
            # Return a minimal schema with timestamp and value
            return pa.schema(
                [
                    pa.field("timestamp", pa.timestamp("ms")),
                    pa.field("value", pa.string()),
                ]
            )

    def _process_batch(self, topic: str, batch: pa.RecordBatch):
        """Process a batch of messages from Kafka."""
        try:
            # Add batch to stream manager
            self.stream_manager.add_batch(topic, batch)

            # Update metrics
            if self.enable_metrics:
                MESSAGES_PROCESSED.labels(topic=topic).inc(batch.num_rows)
                ACTIVE_STREAMS.set(len(self.stream_manager.streams))

            logger.debug(
                f"Processed batch with {batch.num_rows} records for topic {topic}"
            )
        except Exception as e:
            logger.exception(f"Error processing batch for topic {topic}: {e}")

    # Flight server method implementations

    def list_flights(self, context, criteria):
        """
        Implement list_flights to enumerate available streams.

        Args:
            context: Flight server context
            criteria: Optional filter criteria

        Returns:
            Generator of FlightInfo objects
        """
        try:
            # Get all active streams from the stream manager
            flights = self.stream_manager.list_flights()
            return flights
        except Exception as e:
            logger.exception(f"Error listing flights: {e}")
            return []

    def get_flight_info(self, context, descriptor):
        """
        Get information about a specific flight.

        Args:
            context: Flight server context
            descriptor: Flight descriptor

        Returns:
            FlightInfo for the requested stream

        Raises:
            flight.FlightUnavailableError: If the stream is not found
        """
        try:
            stream = self.stream_manager.get_stream_by_descriptor(descriptor)
            if not stream:
                raise flight.FlightUnavailableError(
                    f"Stream not found for descriptor: {descriptor}"
                )

            return stream.get_flight_info()
        except flight.FlightUnavailableError:
            raise
        except Exception as e:
            logger.exception(f"Error getting flight info: {e}")
            raise flight.FlightServerError(f"Internal server error: {e}")

    def do_get(self, context, ticket):
        """
        Serve data for a flight ticket.

        Args:
            context: Flight server context
            ticket: Flight ticket

        Returns:
            Generator of RecordBatch objects for the stream

        Raises:
            flight.FlightUnavailableError: If the stream is not found
        """
        try:
            stream = self.stream_manager.get_stream_by_ticket(ticket)
            if not stream:
                raise flight.FlightUnavailableError(
                    f"Stream not found for ticket: {ticket}"
                )

            # Get batches from the stream
            batches = stream.get_all_batches()

            # Update metrics
            if self.enable_metrics:
                records_count = sum(batch.num_rows for batch in batches)
                RECORDS_SERVED.labels(topic=stream.topic).inc(records_count)

            # Return batches
            for batch in batches:
                yield batch

        except flight.FlightUnavailableError:
            raise
        except Exception as e:
            logger.exception(f"Error serving flight data: {e}")
            raise flight.FlightServerError(f"Internal server error: {e}")

    def do_action(self, context, action):
        """
        Implement Flight actions for controlling Kafka consumption.

        Supported actions:
        - pause-topic: Pause consumption from a topic
        - resume-topic: Resume consumption from a topic
        - seek-topic: Seek to a specific offset for a topic
        - list-topics: List available Kafka topics

        Args:
            context: Flight server context
            action: Flight action

        Returns:
            Generator of result objects

        Raises:
            flight.FlightUnavailableError: If the action is not supported
        """
        try:
            action_type = action.type

            if action_type == "pause-topic":
                # Placeholder for pause-topic action
                # TODO: Implement topic pausing
                body = action.body.to_pybytes()
                topic = body.decode("utf-8")
                return [f"Paused topic: {topic}".encode()]

            elif action_type == "resume-topic":
                # Placeholder for resume-topic action
                # TODO: Implement topic resuming
                body = action.body.to_pybytes()
                topic = body.decode("utf-8")
                return [f"Resumed topic: {topic}".encode()]

            elif action_type == "seek-topic":
                # Placeholder for seek-topic action
                # TODO: Implement topic seeking
                body = json.loads(action.body.to_pybytes().decode("utf-8"))
                topic = body.get("topic")
                offset = body.get("offset")
                return [f"Seek topic {topic} to offset {offset}".encode()]

            elif action_type == "list-topics":
                # List available topics
                # TODO: Implement dynamic topic listing
                return [json.dumps(self.topics or []).encode()]

            else:
                raise flight.FlightUnavailableError(
                    f"Unsupported action: {action_type}"
                )

        except flight.FlightUnavailableError:
            raise
        except Exception as e:
            logger.exception(f"Error performing action: {e}")
            raise flight.FlightServerError(f"Internal server error: {e}")


def serve(
    host: str = "0.0.0.0",
    port: int = 8815,
    kafka_bootstrap_servers: str = "localhost:9092",
    schema_registry_url: Optional[str] = None,
    topics: Optional[List[str]] = None,
    ttl: int = 300,
    batch_size: int = 1000,
    enable_metrics: bool = True,
    metrics_port: int = 8816,
):
    """
    Start the Flare Flight server.

    Args:
        host: Host to bind on
        port: Port to bind on
        kafka_bootstrap_servers: Kafka bootstrap servers
        schema_registry_url: Schema Registry URL (optional)
        topics: List of Kafka topics to consume (None for auto-discovery)
        ttl: Time-to-live for streams in seconds
        batch_size: Maximum number of messages per Arrow batch
        enable_metrics: Whether to enable Prometheus metrics
        metrics_port: Port for Prometheus metrics HTTP server
    """
    location = f"grpc://{host}:{port}"

    # Create and start the server
    server = FlareFlightServer(
        location=location,
        kafka_bootstrap_servers=kafka_bootstrap_servers,
        schema_registry_url=schema_registry_url,
        topics=topics,
        ttl=ttl,
        batch_size=batch_size,
        enable_metrics=enable_metrics,
        metrics_port=metrics_port,
    )

    # Start the server
    server.start()

    # Run the server
    logger.info(f"Starting Flare Flight server on {location}")
    server.serve()
