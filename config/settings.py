from typing import Dict, List, Optional, Any
import os
import re
from dataclasses import dataclass
from enum import Enum
import logging

from flare.schema.resolver import SchemaFormat

logger = logging.getLogger(__name__)


class LogLevel(Enum):
    DEBUG = "DEBUG"
    INFO = "INFO"
    WARNING = "WARNING"
    ERROR = "ERROR"
    CRITICAL = "CRITICAL"


@dataclass
class FlareConfig:
    """Configuration settings for Flare."""

    # Kafka settings
    kafka_bootstrap_servers: str = "localhost:9092"
    kafka_group_id: str = "flare-consumer"
    kafka_auto_offset_reset: str = "latest"

    # Topic settings
    topics: Optional[List[str]] = None
    topic_filter_regex: Optional[str] = None

    # Schema settings
    schema_registry_url: Optional[str] = None
    schema_format: SchemaFormat = SchemaFormat.INFER

    # Flight server settings
    flight_host: str = "0.0.0.0"
    flight_port: int = 8815

    # Stream settings
    stream_ttl: int = 300
    batch_size: int = 1000
    max_batches_per_stream: int = 100

    # Metrics settings
    enable_metrics: bool = True
    metrics_port: int = 8816

    # Logging settings
    log_level: LogLevel = LogLevel.INFO
    log_format: str = "%(asctime)s - %(name)s - %(levelname)s - %(message)s"

    # Persistence settings
    enable_persistence: bool = False
    persistence_path: Optional[str] = None

    @classmethod
    def from_env(cls) -> "FlareConfig":
        """Create config from environment variables."""
        config = cls()

        # Kafka settings
        if "FLARE_KAFKA_BOOTSTRAP_SERVERS" in os.environ:
            config.kafka_bootstrap_servers = os.environ["FLARE_KAFKA_BOOTSTRAP_SERVERS"]

        if "FLARE_KAFKA_GROUP_ID" in os.environ:
            config.kafka_group_id = os.environ["FLARE_KAFKA_GROUP_ID"]

        if "FLARE_KAFKA_AUTO_OFFSET_RESET" in os.environ:
            config.kafka_auto_offset_reset = os.environ["FLARE_KAFKA_AUTO_OFFSET_RESET"]

        # Topic settings
        if "FLARE_TOPICS" in os.environ:
            config.topics = os.environ["FLARE_TOPICS"].split(",")

        if "FLARE_TOPIC_FILTER_REGEX" in os.environ:
            config.topic_filter_regex = os.environ["FLARE_TOPIC_FILTER_REGEX"]

        # Schema settings
        if "FLARE_SCHEMA_REGISTRY_URL" in os.environ:
            config.schema_registry_url = os.environ["FLARE_SCHEMA_REGISTRY_URL"]

        if "FLARE_SCHEMA_FORMAT" in os.environ:
            try:
                config.schema_format = SchemaFormat(os.environ["FLARE_SCHEMA_FORMAT"])
            except ValueError:
                logger.warning(
                    f"Invalid schema format: {os.environ['FLARE_SCHEMA_FORMAT']}, using default"
                )

        # Flight server settings
        if "FLARE_FLIGHT_HOST" in os.environ:
            config.flight_host = os.environ["FLARE_FLIGHT_HOST"]

        if "FLARE_FLIGHT_PORT" in os.environ:
            try:
                config.flight_port = int(os.environ["FLARE_FLIGHT_PORT"])
            except ValueError:
                logger.warning(
                    f"Invalid flight port: {os.environ['FLARE_FLIGHT_PORT']}, using default"
                )

        # Stream settings
        if "FLARE_STREAM_TTL" in os.environ:
            try:
                config.stream_ttl = int(os.environ["FLARE_STREAM_TTL"])
            except ValueError:
                logger.warning(
                    f"Invalid stream TTL: {os.environ['FLARE_STREAM_TTL']}, using default"
                )

        if "FLARE_BATCH_SIZE" in os.environ:
            try:
                config.batch_size = int(os.environ["FLARE_BATCH_SIZE"])
            except ValueError:
                logger.warning(
                    f"Invalid batch size: {os.environ['FLARE_BATCH_SIZE']}, using default"
                )

        if "FLARE_MAX_BATCHES_PER_STREAM" in os.environ:
            try:
                config.max_batches_per_stream = int(
                    os.environ["FLARE_MAX_BATCHES_PER_STREAM"]
                )
            except ValueError:
                logger.warning(
                    f"Invalid max batches per stream: {os.environ['FLARE_MAX_BATCHES_PER_STREAM']}, using default"
                )

        # Metrics settings
        if "FLARE_ENABLE_METRICS" in os.environ:
            config.enable_metrics = os.environ["FLARE_ENABLE_METRICS"].lower() in (
                "true",
                "yes",
                "1",
            )

        if "FLARE_METRICS_PORT" in os.environ:
            try:
                config.metrics_port = int(os.environ["FLARE_METRICS_PORT"])
            except ValueError:
                logger.warning(
                    f"Invalid metrics port: {os.environ['FLARE_METRICS_PORT']}, using default"
                )

        # Logging settings
        if "FLARE_LOG_LEVEL" in os.environ:
            try:
                config.log_level = LogLevel(os.environ["FLARE_LOG_LEVEL"])
            except ValueError:
                logger.warning(
                    f"Invalid log level: {os.environ['FLARE_LOG_LEVEL']}, using default"
                )

        if "FLARE_LOG_FORMAT" in os.environ:
            config.log_format = os.environ["FLARE_LOG_FORMAT"]

        # Persistence settings
        if "FLARE_ENABLE_PERSISTENCE" in os.environ:
            config.enable_persistence = os.environ[
                "FLARE_ENABLE_PERSISTENCE"
            ].lower() in ("true", "yes", "1")

        if "FLARE_PERSISTENCE_PATH" in os.environ:
            config.persistence_path = os.environ["FLARE_PERSISTENCE_PATH"]

        return config

    def filter_topic(self, topic: str) -> bool:
        """Check if a topic should be included based on filter settings."""
        # If topics list is specified, check against it
        if self.topics:
            return topic in self.topics

        # If regex filter is specified, check against it
        if self.topic_filter_regex:
            try:
                return bool(re.match(self.topic_filter_regex, topic))
            except re.error:
                logger.error(f"Invalid topic filter regex: {self.topic_filter_regex}")
                return False

        # No filter, include all topics
        return True

    def setup_logging(self):
        """Configure logging based on settings."""
        log_level = getattr(logging, self.log_level.value)

        logging.basicConfig(level=log_level, format=self.log_format)

        # Set Flask and Werkzeug loggers to WARNING to reduce noise
        logging.getLogger("werkzeug").setLevel(logging.WARNING)

        logger.info(f"Logging configured with level {self.log_level.value}")


# Default configuration
default_config = FlareConfig()
