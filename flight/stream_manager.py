from typing import Dict, List, Optional, Any, Callable, Tuple
import time
import threading
import logging
import uuid
from collections import deque
import pyarrow as pa
import pyarrow.flight as flight

logger = logging.getLogger(__name__)


class StreamInfo:
    """Information about a stream associated with a Kafka topic."""

    def __init__(
        self,
        topic: str,
        descriptor: flight.FlightDescriptor,
        ttl: int = 300,  # Time-to-live in seconds
        max_batches: int = 100,  # Maximum number of batches to keep in memory
    ):
        self.topic = topic
        self.descriptor = descriptor
        self.ttl = ttl
        self.max_batches = max_batches

        self.created_at = time.time()
        self.last_accessed = time.time()
        self.active = True

        # Buffer of record batches
        self.batches: deque[pa.RecordBatch] = deque(maxlen=max_batches)

        # Schema for the stream
        self.schema: Optional[pa.Schema] = None

        # Total number of records in this stream
        self.record_count = 0

    def add_batch(self, batch: pa.RecordBatch) -> None:
        """Add a batch to the stream."""
        if not self.active:
            return

        self.batches.append(batch)
        self.last_accessed = time.time()

        # Set schema from first batch if not already set
        if self.schema is None and batch is not None:
            self.schema = batch.schema

        # Update record count
        self.record_count += batch.num_rows

    def is_expired(self) -> bool:
        """Check if the stream has expired based on TTL."""
        return time.time() - self.last_accessed > self.ttl

    def mark_accessed(self) -> None:
        """Mark the stream as recently accessed."""
        self.last_accessed = time.time()

    def get_all_batches(self) -> List[pa.RecordBatch]:
        """Get all batches in the stream."""
        self.mark_accessed()
        return list(self.batches)

    def get_flight_info(self) -> flight.FlightInfo:
        """Get Flight info for this stream."""
        if not self.schema:
            # If no schema set yet, use empty schema
            self.schema = pa.schema([])

        # Create a FlightEndpoint for this stream
        endpoint = flight.FlightEndpoint(
            ticket=flight.Ticket(self.descriptor.command),
            locations=[],  # No explicit location - handled by the same server
        )

        # Create FlightInfo
        return flight.FlightInfo(
            schema=self.schema,
            descriptor=self.descriptor,
            endpoints=[endpoint],
            total_records=self.record_count,
            total_bytes=-1,  # Unknown total bytes
        )


class FlightStreamManager:
    """
    Manages ephemeral Flight streams for Kafka topics.
    """

    def __init__(
        self,
        default_ttl: int = 300,
        max_batches_per_stream: int = 100,
        cleanup_interval: int = 60,
    ):
        """
        Initialize the Flight stream manager.

        Args:
            default_ttl: Default time-to-live for streams in seconds
            max_batches_per_stream: Maximum batches to keep per stream
            cleanup_interval: Interval in seconds for cleanup of expired streams
        """
        self.default_ttl = default_ttl
        self.max_batches_per_stream = max_batches_per_stream
        self.cleanup_interval = cleanup_interval

        # Map of topic -> StreamInfo
        self.streams: Dict[str, StreamInfo] = {}

        # Map of descriptor command -> topic
        self.descriptor_to_topic: Dict[str, str] = {}

        # Lock for thread-safe access to streams
        self.lock = threading.RLock()

        # Start cleanup thread
        self.running = True
        self.cleanup_thread = threading.Thread(target=self._cleanup_loop, daemon=True)
        self.cleanup_thread.start()

    def stop(self):
        """Stop the manager and cleanup thread."""
        self.running = False
        if self.cleanup_thread:
            self.cleanup_thread.join(timeout=5.0)

    def create_or_get_stream(self, topic: str) -> StreamInfo:
        """
        Create a new stream or return an existing one for a topic.

        Args:
            topic: Kafka topic name

        Returns:
            StreamInfo for the topic
        """
        with self.lock:
            # Return existing stream if available
            if topic in self.streams and self.streams[topic].active:
                self.streams[topic].mark_accessed()
                return self.streams[topic]

            # Create a new descriptor for this topic
            descriptor = flight.FlightDescriptor.for_command(topic)

            # Create a new stream
            stream = StreamInfo(
                topic=topic,
                descriptor=descriptor,
                ttl=self.default_ttl,
                max_batches=self.max_batches_per_stream,
            )

            # Register the stream
            self.streams[topic] = stream
            self.descriptor_to_topic[topic] = topic  # Simple mapping for now

            logger.info(f"Created new stream for topic {topic}")
            return stream

    def add_batch(self, topic: str, batch: pa.RecordBatch) -> None:
        """
        Add a batch to a topic's stream.

        Args:
            topic: Kafka topic name
            batch: Arrow RecordBatch to add
        """
        with self.lock:
            stream = self.create_or_get_stream(topic)
            stream.add_batch(batch)

    def get_stream_by_descriptor(
        self, descriptor: flight.FlightDescriptor
    ) -> Optional[StreamInfo]:
        """
        Get a stream by its Flight descriptor.

        Args:
            descriptor: Flight descriptor

        Returns:
            StreamInfo if found, None otherwise
        """
        if descriptor.type != flight.DescriptorType.CMD:
            return None

        command = descriptor.command.decode("utf-8")

        with self.lock:
            if command in self.descriptor_to_topic:
                topic = self.descriptor_to_topic[command]
                if topic in self.streams:
                    self.streams[topic].mark_accessed()
                    return self.streams[topic]

        return None

    def get_stream_by_ticket(self, ticket: flight.Ticket) -> Optional[StreamInfo]:
        """
        Get a stream by its Flight ticket.

        Args:
            ticket: Flight ticket

        Returns:
            StreamInfo if found, None otherwise
        """
        command = ticket.ticket.decode("utf-8")

        with self.lock:
            if command in self.descriptor_to_topic:
                topic = self.descriptor_to_topic[command]
                if topic in self.streams:
                    self.streams[topic].mark_accessed()
                    return self.streams[topic]

        return None

    def list_flights(self) -> List[flight.FlightInfo]:
        """
        List all active flights.

        Returns:
            List of FlightInfo objects for all active streams
        """
        with self.lock:
            result = []
            for topic, stream in self.streams.items():
                if stream.active and not stream.is_expired():
                    result.append(stream.get_flight_info())
            return result

    def _cleanup_loop(self):
        """Background loop to clean up expired streams."""
        while self.running:
            try:
                self._cleanup_expired_streams()
            except Exception as e:
                logger.exception(f"Error in stream cleanup: {e}")

            # Sleep before next cleanup
            for _ in range(self.cleanup_interval):
                if not self.running:
                    break
                time.sleep(1)

    def _cleanup_expired_streams(self):
        """Clean up expired streams."""
        expired_topics = []

        with self.lock:
            # Find expired streams
            for topic, stream in self.streams.items():
                if stream.is_expired():
                    expired_topics.append(topic)
                    logger.info(
                        f"Stream for topic {topic} expired after {self.default_ttl} seconds of inactivity"
                    )

            # Remove expired streams
            for topic in expired_topics:
                stream = self.streams[topic]
                stream.active = False
                # Remove from descriptor mapping
                if topic in self.descriptor_to_topic:
                    del self.descriptor_to_topic[topic]

                # Keep the stream object in self.streams for potential reuse
                # but mark it as inactive
