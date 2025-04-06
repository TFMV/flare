from typing import Dict, List, Optional, Any, Union, Callable
import json
import logging
import requests
from enum import Enum
import pyarrow as pa

logger = logging.getLogger(__name__)


class SchemaFormat(Enum):
    AVRO = "avro"
    PROTOBUF = "protobuf"
    JSON = "json"
    INFER = "infer"


class SchemaResolver:
    """
    Resolves schemas for Kafka topics from a Schema Registry or infers them from sample messages.
    """

    def __init__(
        self,
        schema_registry_url: Optional[str] = None,
        default_format: SchemaFormat = SchemaFormat.JSON,
        cache_schemas: bool = True,
    ):
        """
        Initialize the schema resolver.

        Args:
            schema_registry_url: URL of the Schema Registry (None to disable)
            default_format: Default schema format for registry lookups
            cache_schemas: Whether to cache resolved schemas
        """
        self.schema_registry_url = schema_registry_url
        self.default_format = default_format
        self.cache_schemas = cache_schemas

        # Cache of topic -> schema
        self.schema_cache: Dict[str, pa.Schema] = {}

    def get_schema(
        self,
        topic: str,
        sample_messages: Optional[List[Any]] = None,
        format_override: Optional[SchemaFormat] = None,
    ) -> pa.Schema:
        """
        Get schema for a topic, either from registry or by inferring from sample messages.

        Args:
            topic: Kafka topic name
            sample_messages: Sample messages for schema inference (if needed)
            format_override: Override the default schema format

        Returns:
            PyArrow Schema for the topic

        Raises:
            ValueError: If schema resolution fails
        """
        # Check cache first
        if self.cache_schemas and topic in self.schema_cache:
            return self.schema_cache[topic]

        schema_format = format_override or self.default_format

        # Try schema registry first if configured
        if self.schema_registry_url and schema_format != SchemaFormat.INFER:
            try:
                schema = self._get_schema_from_registry(topic, schema_format)
                if schema:
                    if self.cache_schemas:
                        self.schema_cache[topic] = schema
                    return schema
            except Exception as e:
                logger.warning(
                    f"Failed to get schema from registry for topic {topic}: {e}"
                )
                # Fall back to inference if registry lookup fails

        # Fall back to schema inference if we have sample messages
        if sample_messages:
            schema = self._infer_schema_from_messages(topic, sample_messages)
            if schema:
                if self.cache_schemas:
                    self.schema_cache[topic] = schema
                return schema

        raise ValueError(f"Failed to resolve schema for topic {topic}")

    def _get_schema_from_registry(
        self, topic: str, schema_format: SchemaFormat
    ) -> pa.Schema:
        """
        Get schema from Schema Registry.

        Args:
            topic: Kafka topic name
            schema_format: Schema format (AVRO, PROTOBUF, JSON)

        Returns:
            PyArrow Schema for the topic

        Raises:
            ValueError: If schema retrieval or conversion fails
        """
        if not self.schema_registry_url:
            raise ValueError("Schema Registry URL not configured")

        # Format subject name based on topic (topic-key or topic-value)
        subject = f"{topic}-value"

        # Fetch latest schema version
        url = f"{self.schema_registry_url}/subjects/{subject}/versions/latest"
        response = requests.get(url)

        if response.status_code != 200:
            raise ValueError(
                f"Failed to get schema: {response.status_code} {response.text}"
            )

        schema_data = response.json()
        schema_str = schema_data.get("schema")

        if not schema_str:
            raise ValueError("Schema not found in registry response")

        # Convert schema based on format
        if schema_format == SchemaFormat.AVRO:
            return self._convert_avro_schema(schema_str)
        elif schema_format == SchemaFormat.PROTOBUF:
            return self._convert_protobuf_schema(schema_str)
        elif schema_format == SchemaFormat.JSON:
            return self._convert_json_schema(schema_str)
        else:
            raise ValueError(f"Unsupported schema format: {schema_format}")

    def _infer_schema_from_messages(self, topic: str, messages: List[Any]) -> pa.Schema:
        """
        Infer schema from sample messages.

        Args:
            topic: Kafka topic name
            messages: List of sample messages

        Returns:
            PyArrow Schema for the topic

        Raises:
            ValueError: If schema inference fails
        """
        if not messages:
            raise ValueError("No sample messages provided for schema inference")

        try:
            # For JSON messages, parse if needed
            parsed_messages = []
            for msg in messages:
                if isinstance(msg, bytes):
                    parsed_msg = json.loads(msg.decode("utf-8"))
                elif isinstance(msg, str):
                    parsed_msg = json.loads(msg)
                else:
                    parsed_msg = msg
                parsed_messages.append(parsed_msg)

            # Use PyArrow's table inference
            table = pa.Table.from_pylist(parsed_messages)
            return table.schema

        except Exception as e:
            logger.exception(f"Failed to infer schema for topic {topic}: {e}")
            raise ValueError(f"Schema inference failed: {e}")

    def _convert_avro_schema(self, schema_str: str) -> pa.Schema:
        """Convert Avro schema to PyArrow schema."""
        # Parse Avro schema
        avro_schema = json.loads(schema_str)

        # Map Avro types to PyArrow types
        fields = []

        # Process record fields
        if avro_schema.get("type") == "record" and "fields" in avro_schema:
            for field in avro_schema["fields"]:
                name = field["name"]
                avro_type = field["type"]

                # Handle primitive types
                if avro_type == "string":
                    pa_type = pa.string()
                elif avro_type == "int":
                    pa_type = pa.int32()
                elif avro_type == "long":
                    pa_type = pa.int64()
                elif avro_type == "float":
                    pa_type = pa.float32()
                elif avro_type == "double":
                    pa_type = pa.float64()
                elif avro_type == "boolean":
                    pa_type = pa.bool_()
                elif avro_type == "bytes":
                    pa_type = pa.binary()
                elif isinstance(avro_type, list) and "null" in avro_type:
                    # Handle nullable types (union with null)
                    non_null_types = [t for t in avro_type if t != "null"]
                    if len(non_null_types) == 1:
                        # Simple nullable type
                        if non_null_types[0] == "string":
                            pa_type = pa.string()
                        elif non_null_types[0] == "int":
                            pa_type = pa.int32()
                        elif non_null_types[0] == "long":
                            pa_type = pa.int64()
                        elif non_null_types[0] == "float":
                            pa_type = pa.float32()
                        elif non_null_types[0] == "double":
                            pa_type = pa.float64()
                        elif non_null_types[0] == "boolean":
                            pa_type = pa.bool_()
                        elif non_null_types[0] == "bytes":
                            pa_type = pa.binary()
                        else:
                            # Default to string for complex types
                            pa_type = pa.string()
                    else:
                        # Default to string for complex unions
                        pa_type = pa.string()
                else:
                    # Default to string for complex types
                    pa_type = pa.string()

                fields.append(pa.field(name, pa_type))

        return pa.schema(fields)

    def _convert_protobuf_schema(self, schema_str: str) -> pa.Schema:
        """Convert Protobuf schema to PyArrow schema."""
        # This is a simplified placeholder
        # A full implementation would parse the protobuf schema and map to PyArrow types
        logger.warning(
            "Protobuf schema conversion is simplified and may not be accurate"
        )

        # For now, parse as JSON and extract fields
        proto_schema = json.loads(schema_str)

        # Very basic implementation - a real one would parse protobuf definitions
        fields = []
        # Placeholder implementation
        return pa.schema(fields)

    def _convert_json_schema(self, schema_str: str) -> pa.Schema:
        """Convert JSON schema to PyArrow schema."""
        json_schema = json.loads(schema_str)

        # Get properties from JSON schema
        properties = json_schema.get("properties", {})

        fields = []
        for prop_name, prop_def in properties.items():
            prop_type = prop_def.get("type")

            # Map JSON schema types to PyArrow types
            if prop_type == "string":
                pa_type = pa.string()
            elif prop_type == "integer":
                pa_type = pa.int64()
            elif prop_type == "number":
                pa_type = pa.float64()
            elif prop_type == "boolean":
                pa_type = pa.bool_()
            elif prop_type == "array":
                # Simple array handling - assume array of strings for now
                items_type = prop_def.get("items", {}).get("type", "string")
                if items_type == "string":
                    pa_type = pa.list_(pa.string())
                elif items_type == "integer":
                    pa_type = pa.list_(pa.int64())
                elif items_type == "number":
                    pa_type = pa.list_(pa.float64())
                elif items_type == "boolean":
                    pa_type = pa.list_(pa.bool_())
                else:
                    pa_type = pa.list_(pa.string())
            elif prop_type == "object":
                # Simple handling - convert to string for now
                # A full implementation would create nested structs
                pa_type = pa.string()
            else:
                # Default to string for unknown types
                pa_type = pa.string()

            fields.append(pa.field(prop_name, pa_type))

        return pa.schema(fields)
