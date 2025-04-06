from typing import Dict, List, Optional, Any, Callable
import threading
import logging
import time
from confluent_kafka import Consumer, KafkaError, TopicPartition
import pyarrow as pa

logger = logging.getLogger(__name__)


class KafkaIngestLayer:
    """
    IngestLayer that consumes messages from Kafka topics and converts them to Arrow RecordBatches.
    """

    def __init__(
        self,
        bootstrap_servers: str,
        topics: Optional[List[str]] = None,
        group_id: str = "flare-consumer",
        batch_size: int = 1000,
        poll_timeout: float = 1.0,
        auto_offset_reset: str = "latest",
        schema_callback: Optional[Callable[[str], pa.Schema]] = None,
    ):
        """
        Initialize the Kafka ingest layer.

        Args:
            bootstrap_servers: Kafka bootstrap servers
            topics: List of topics to subscribe to (None for dynamic discovery)
            group_id: Kafka consumer group ID
            batch_size: Maximum number of messages per Arrow batch
            poll_timeout: Timeout for polling messages (seconds)
            auto_offset_reset: Offset reset strategy ("earliest" or "latest")
            schema_callback: Callback to retrieve schema for a topic
        """
        self.bootstrap_servers = bootstrap_servers
        self.topics = topics
        self.group_id = group_id
        self.batch_size = batch_size
        self.poll_timeout = poll_timeout
        self.auto_offset_reset = auto_offset_reset
        self.schema_callback = schema_callback

        self.consumer: Optional[Consumer] = None
        self.running = False
        self.consumer_thread: Optional[threading.Thread] = None

        # Map of topic -> latest offset for each partition
        self.offsets: Dict[str, Dict[int, int]] = {}

        # Map of topic -> callback for receiving batches
        self.batch_callbacks: Dict[str, Callable[[str, pa.RecordBatch], None]] = {}

        # Topic to schema mapping
        self.topic_schemas: Dict[str, pa.Schema] = {}

    def start(self):
        """Start the Kafka consumer thread."""
        if self.running:
            return

        self.consumer = Consumer(
            {
                "bootstrap.servers": self.bootstrap_servers,
                "group.id": self.group_id,
                "auto.offset.reset": self.auto_offset_reset,
                "enable.auto.commit": False,  # We'll handle commits manually
            }
        )

        if self.topics:
            self.consumer.subscribe(self.topics)
        else:
            # Dynamic topic discovery
            metadata = self.consumer.list_topics(timeout=10)
            available_topics = [topic for topic in metadata.topics]
            logger.info(f"Discovered topics: {available_topics}")
            self.consumer.subscribe(available_topics)

        self.running = True
        self.consumer_thread = threading.Thread(target=self._consume_loop, daemon=True)
        self.consumer_thread.start()
        logger.info(f"Started Kafka consumer for {self.bootstrap_servers}")

    def stop(self):
        """Stop the Kafka consumer thread."""
        self.running = False
        if self.consumer_thread:
            self.consumer_thread.join(timeout=5.0)
        if self.consumer:
            self.consumer.close()
            self.consumer = None
        logger.info("Stopped Kafka consumer")

    def register_batch_callback(
        self, topic: str, callback: Callable[[str, pa.RecordBatch], None]
    ):
        """
        Register a callback to receive batches for a topic.

        Args:
            topic: Kafka topic
            callback: Function to call with (topic, batch) when a batch is ready
        """
        self.batch_callbacks[topic] = callback
        logger.info(f"Registered batch callback for topic {topic}")

    def _consume_loop(self):
        """Main consumption loop."""
        if not self.consumer:
            return

        # Message buffers per topic
        buffers: Dict[str, List[Any]] = {}

        while self.running:
            try:
                msg = self.consumer.poll(self.poll_timeout)

                if msg is None:
                    # Process any partially filled batches if we've been waiting too long
                    now = time.time()
                    for topic, messages in list(buffers.items()):
                        if messages:
                            batch = self._create_batch(topic, messages)
                            if batch and topic in self.batch_callbacks:
                                self.batch_callbacks[topic](topic, batch)
                            buffers[topic] = []
                    continue

                if msg.error():
                    if msg.error().code() == KafkaError._PARTITION_EOF:
                        # End of partition, not an error
                        continue
                    logger.error(f"Kafka error: {msg.error()}")
                    continue

                topic = msg.topic()

                # Initialize buffer for this topic if needed
                if topic not in buffers:
                    buffers[topic] = []

                # Get schema for this topic if we don't have it yet
                if topic not in self.topic_schemas and self.schema_callback:
                    try:
                        self.topic_schemas[topic] = self.schema_callback(topic)
                    except Exception as e:
                        logger.error(f"Failed to get schema for topic {topic}: {e}")

                # Add message to buffer
                buffers[topic].append(msg.value())

                # Track offset
                partition = msg.partition()
                offset = msg.offset()
                if topic not in self.offsets:
                    self.offsets[topic] = {}
                self.offsets[topic][partition] = offset

                # Create batch if we've reached batch size
                if len(buffers[topic]) >= self.batch_size:
                    batch = self._create_batch(topic, buffers[topic])
                    if batch and topic in self.batch_callbacks:
                        self.batch_callbacks[topic](topic, batch)
                    buffers[topic] = []

                    # Commit offset
                    self.consumer.commit(msg, asynchronous=True)

            except Exception as e:
                logger.exception(f"Error in Kafka consumer loop: {e}")
                time.sleep(1)  # Avoid spinning in case of persistent errors

    def _create_batch(
        self, topic: str, messages: List[Any]
    ) -> Optional[pa.RecordBatch]:
        """
        Create an Arrow RecordBatch from a list of messages.

        Args:
            topic: Kafka topic
            messages: List of message values from Kafka

        Returns:
            Arrow RecordBatch or None if creation fails
        """
        if not messages:
            return None

        try:
            schema = self.topic_schemas.get(topic)
            if not schema and self.schema_callback:
                schema = self.schema_callback(topic)
                self.topic_schemas[topic] = schema

            if not schema:
                logger.warning(
                    f"No schema available for topic {topic}, skipping batch creation"
                )
                return None

            # Convert messages to Arrow RecordBatch (implementation depends on message format)
            # This is a simplified placeholder - actual implementation would depend on format
            arrays = []
            for field in schema.names:
                # Placeholder for actual conversion logic
                arrays.append(
                    pa.array(
                        [msg.get(field) for msg in messages], schema.field(field).type
                    )
                )

            return pa.RecordBatch.from_arrays(arrays, schema)

        except Exception as e:
            logger.exception(f"Failed to create batch for topic {topic}: {e}")
            return None
