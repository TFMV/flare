from typing import Dict, List, Optional, Any, Union
import logging
import os
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import datetime
import time
import uuid

try:
    from pyiceberg.catalog import load_catalog
    from pyiceberg.exceptions import NoSuchTableError
    from pyiceberg.schema import Schema as IcebergSchema
    from pyiceberg.types import (
        BooleanType,
        IntegerType,
        LongType,
        FloatType,
        DoubleType,
        StringType,
        BinaryType,
        TimestampType,
        ListType,
        MapType,
        StructType,
    )
    from pyiceberg.table import Table as IcebergTable
    from pyiceberg.table.sorting import SortOrder
    from pyiceberg.partitioning import PartitionSpec
    import pyiceberg.expressions as expressions

    HAS_PYICEBERG = True
except ImportError:
    HAS_PYICEBERG = False

logger = logging.getLogger(__name__)


class IcebergPersistence:
    """
    Persistence layer using Apache Iceberg.

    Integrates with pyiceberg library to store Arrow RecordBatches into Iceberg tables.
    If pyiceberg is not available, falls back to writing Parquet files in a directory structure.
    """

    def __init__(
        self,
        base_path: str,
        catalog_name: str = "flare",
        warehouse_path: Optional[str] = None,
        catalog_properties: Optional[Dict[str, str]] = None,
        namespace: str = "default",
    ):
        """
        Initialize the Iceberg persistence layer.

        Args:
            base_path: Base path for Iceberg data
            catalog_name: Name of the Iceberg catalog
            warehouse_path: Warehouse path for Iceberg catalog
            catalog_properties: Additional properties for the catalog
            namespace: Default namespace for tables
        """
        self.base_path = base_path
        self.catalog_name = catalog_name
        self.warehouse_path = warehouse_path or os.path.join(base_path, "warehouse")
        self.namespace = namespace

        # Create directories if they don't exist
        os.makedirs(self.base_path, exist_ok=True)
        os.makedirs(self.warehouse_path, exist_ok=True)

        # Dictionary of topic -> schema to track table schemas
        self.topic_schemas: Dict[str, pa.Schema] = {}

        # Try to init pyiceberg catalog
        self.catalog = None
        if HAS_PYICEBERG:
            try:
                properties = catalog_properties or {}
                properties.update(
                    {
                        "warehouse": self.warehouse_path,
                        "type": "hadoop",  # Can be overridden in catalog_properties
                    }
                )

                self.catalog = load_catalog(self.catalog_name, **properties)
                logger.info(
                    f"Initialized Iceberg catalog '{catalog_name}' at {self.warehouse_path}"
                )
            except Exception as e:
                logger.warning(f"Failed to initialize Iceberg catalog: {e}")
                self.catalog = None
        else:
            logger.warning(
                "pyiceberg not installed, using simplified Parquet-based persistence"
            )

    def persist_batch(
        self,
        topic: str,
        batch: pa.RecordBatch,
        partition_by: Optional[List[str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """
        Persist a batch of records to an Iceberg table.

        Args:
            topic: Kafka topic (used as table name)
            batch: Arrow RecordBatch to persist
            partition_by: Optional list of columns to partition by
            table_properties: Properties to set on the Iceberg table

        Returns:
            True if persistence succeeded, False otherwise
        """
        # Track the schema for this topic
        if topic not in self.topic_schemas:
            self.topic_schemas[topic] = batch.schema
            logger.info(f"Added schema for topic {topic}")

        if HAS_PYICEBERG and self.catalog:
            return self._persist_batch_with_pyiceberg(
                topic, batch, partition_by, table_properties
            )
        else:
            return self._persist_batch_fallback(topic, batch)

    def _persist_batch_with_pyiceberg(
        self,
        topic: str,
        batch: pa.RecordBatch,
        partition_by: Optional[List[str]] = None,
        table_properties: Optional[Dict[str, str]] = None,
    ) -> bool:
        """Use pyiceberg to persist the batch to an Iceberg table."""
        try:
            table_identifier = f"{self.namespace}.{topic}"

            # Get or create the table
            try:
                table = self.catalog.load_table(table_identifier)
                logger.debug(f"Loaded existing table {table_identifier}")
            except NoSuchTableError:
                # Create the table with schema derived from the Arrow schema
                iceberg_schema = self._arrow_to_iceberg_schema(batch.schema)

                # Create partition spec if partitioning columns are specified
                if partition_by and len(partition_by) > 0:
                    partition_spec = PartitionSpec.builder()
                    for col in partition_by:
                        partition_spec = partition_spec.identity(col)
                    partition_spec = partition_spec.build()
                else:
                    partition_spec = None

                # Set table properties
                props = table_properties or {}
                props.update(
                    {
                        "write.format.default": "parquet",
                        "flare.source.topic": topic,
                        "flare.created.at": str(int(time.time())),
                    }
                )

                # Create the table
                table = self.catalog.create_table(
                    identifier=table_identifier,
                    schema=iceberg_schema,
                    partition_spec=partition_spec,
                    properties=props,
                    sort_order=SortOrder.unsorted(),
                )
                logger.info(f"Created new Iceberg table {table_identifier}")

            # Convert Arrow RecordBatch to PyArrow Table
            pa_table = pa.Table.from_batches([batch])

            # Create a temp file to store the data
            batch_id = str(uuid.uuid4())
            temp_file = os.path.join(self.base_path, f"temp_{topic}_{batch_id}.parquet")

            try:
                # Write the batch to a temporary Parquet file
                pq.write_table(pa_table, temp_file)

                # Get the data files from the temp Parquet file
                data_files = self._get_data_files_from_parquet(temp_file, table)

                # Append data to the table
                table.append(data_files)

                logger.info(
                    f"Persisted batch with {batch.num_rows} records to Iceberg table {table_identifier}"
                )
                return True
            finally:
                # Clean up the temporary file
                if os.path.exists(temp_file):
                    os.remove(temp_file)

        except Exception as e:
            logger.exception(
                f"Failed to persist batch to Iceberg table for topic {topic}: {e}"
            )
            return False

    def _get_data_files_from_parquet(
        self, parquet_file: str, table: "IcebergTable"
    ) -> List[Dict[str, Any]]:
        """Extract data files from a Parquet file for appending to an Iceberg table."""
        # This is a simplified approach - in a real implementation, you would:
        # 1. Read the file's metadata
        # 2. Determine partitioning
        # 3. Create appropriate data file records for Iceberg

        # Placeholder implementation - in a real case, this would need to be expanded
        # with proper handling of partitioning and file statistics
        return [
            {
                "content": 0,  # 0 for data, 1 for delete, etc.
                "file_path": parquet_file,
                "file_format": "PARQUET",
                "partition": {},  # Partitioning info
                "record_count": pq.read_metadata(parquet_file).num_rows,
                "file_size_in_bytes": os.path.getsize(parquet_file),
            }
        ]

    def _persist_batch_fallback(self, topic: str, batch: pa.RecordBatch) -> bool:
        """Fallback persistence without pyiceberg - just write Parquet files."""
        try:
            # Create a directory for the topic
            table_path = os.path.join(self.warehouse_path, topic)
            os.makedirs(table_path, exist_ok=True)

            # Create a timestamp-based partition file
            timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
            filename = f"{topic}_{timestamp}.parquet"
            file_path = os.path.join(table_path, filename)

            # Write the batch as a Parquet file
            table = pa.Table.from_batches([batch])
            pq.write_table(table, file_path)

            logger.info(
                f"Persisted batch with {batch.num_rows} records for topic {topic} to {file_path}"
            )
            return True

        except Exception as e:
            logger.exception(f"Failed to persist batch for topic {topic}: {e}")
            return False

    def _arrow_to_iceberg_schema(self, arrow_schema: pa.Schema) -> IcebergSchema:
        """Convert an Arrow schema to an Iceberg schema."""
        iceberg_fields = []

        for field in arrow_schema:
            iceberg_fields.append(self._arrow_field_to_iceberg_field(field))

        return IcebergSchema(*iceberg_fields)

    def _arrow_field_to_iceberg_field(self, arrow_field: pa.Field) -> Dict[str, Any]:
        """Convert an Arrow field to an Iceberg field."""
        name = arrow_field.name
        nullable = arrow_field.nullable
        arrow_type = arrow_field.type

        iceberg_type = self._arrow_type_to_iceberg_type(arrow_type)

        return {
            "name": name,
            "field_id": None,
            "type": iceberg_type,
            "required": not nullable,
        }

    def _arrow_type_to_iceberg_type(self, arrow_type: pa.DataType) -> Any:
        """Map Arrow data types to Iceberg data types."""
        if pa.types.is_boolean(arrow_type):
            return BooleanType()
        elif (
            pa.types.is_int8(arrow_type)
            or pa.types.is_int16(arrow_type)
            or pa.types.is_int32(arrow_type)
        ):
            return IntegerType()
        elif pa.types.is_int64(arrow_type):
            return LongType()
        elif pa.types.is_float32(arrow_type):
            return FloatType()
        elif pa.types.is_float64(arrow_type):
            return DoubleType()
        elif pa.types.is_string(arrow_type):
            return StringType()
        elif pa.types.is_binary(arrow_type):
            return BinaryType()
        elif pa.types.is_timestamp(arrow_type):
            return TimestampType()
        elif pa.types.is_list(arrow_type):
            value_type = self._arrow_type_to_iceberg_type(arrow_type.value_type)
            return ListType(value_type)
        elif pa.types.is_struct(arrow_type):
            fields = []
            for field in arrow_type:
                fields.append(self._arrow_field_to_iceberg_field(field))
            return StructType(*fields)
        elif pa.types.is_map(arrow_type):
            key_type = self._arrow_type_to_iceberg_type(arrow_type.key_type)
            item_type = self._arrow_type_to_iceberg_type(arrow_type.item_type)
            return MapType(key_type, item_type)
        else:
            # Default to string for unsupported types
            logger.warning(
                f"Unsupported Arrow type: {arrow_type}, defaulting to string"
            )
            return StringType()

    def get_table_location(self, topic: str) -> str:
        """
        Get the location of an Iceberg table for a topic.

        Args:
            topic: Kafka topic (used as table name)

        Returns:
            Path to the Iceberg table
        """
        if HAS_PYICEBERG and self.catalog:
            try:
                table = self.catalog.load_table(f"{self.namespace}.{topic}")
                return table.location()
            except Exception:
                pass

        # Fallback if no pyiceberg or table loading failed
        return os.path.join(self.warehouse_path, topic)

    def table_exists(self, topic: str) -> bool:
        """
        Check if an Iceberg table exists for a topic.

        Args:
            topic: Kafka topic (used as table name)

        Returns:
            True if the table exists, False otherwise
        """
        if HAS_PYICEBERG and self.catalog:
            try:
                return self.catalog.table_exists(f"{self.namespace}.{topic}")
            except Exception:
                pass

        # Fallback if no pyiceberg or check failed
        table_path = os.path.join(self.warehouse_path, topic)
        return os.path.exists(table_path)

    def get_table_schema(self, topic: str) -> Optional[pa.Schema]:
        """
        Get the schema for an Iceberg table.

        Args:
            topic: Kafka topic (used as table name)

        Returns:
            Arrow Schema for the table, or None if not found
        """
        # First check our schema cache
        if topic in self.topic_schemas:
            return self.topic_schemas[topic]

        # Try to get schema from Iceberg
        if HAS_PYICEBERG and self.catalog:
            try:
                table = self.catalog.load_table(f"{self.namespace}.{topic}")
                iceberg_schema = table.schema()
                arrow_schema = self._iceberg_to_arrow_schema(iceberg_schema)
                self.topic_schemas[topic] = arrow_schema
                return arrow_schema
            except Exception as e:
                logger.warning(
                    f"Failed to get schema from Iceberg for topic {topic}: {e}"
                )

        # Check if we have Parquet files in the fallback location
        table_path = os.path.join(self.warehouse_path, topic)
        if os.path.exists(table_path):
            parquet_files = [
                f for f in os.listdir(table_path) if f.endswith(".parquet")
            ]
            if parquet_files:
                first_file = os.path.join(table_path, parquet_files[0])
                try:
                    schema = pq.read_schema(first_file)
                    self.topic_schemas[topic] = schema
                    return schema
                except Exception as e:
                    logger.warning(
                        f"Failed to read schema from Parquet file for topic {topic}: {e}"
                    )

        return None

    def _iceberg_to_arrow_schema(self, iceberg_schema: "IcebergSchema") -> pa.Schema:
        """Convert an Iceberg schema to an Arrow schema."""
        fields = []

        for field in iceberg_schema.fields:
            arrow_field = self._iceberg_field_to_arrow_field(field)
            fields.append(arrow_field)

        return pa.schema(fields)

    def _iceberg_field_to_arrow_field(self, iceberg_field: Dict[str, Any]) -> pa.Field:
        """Convert an Iceberg field to an Arrow field."""
        name = iceberg_field["name"]
        nullable = not iceberg_field["required"]
        iceberg_type = iceberg_field["type"]

        arrow_type = self._iceberg_type_to_arrow_type(iceberg_type)

        return pa.field(name, arrow_type, nullable=nullable)

    def _iceberg_type_to_arrow_type(self, iceberg_type: Any) -> pa.DataType:
        """Map Iceberg data types to Arrow data types."""
        if isinstance(iceberg_type, BooleanType):
            return pa.bool_()
        elif isinstance(iceberg_type, IntegerType):
            return pa.int32()
        elif isinstance(iceberg_type, LongType):
            return pa.int64()
        elif isinstance(iceberg_type, FloatType):
            return pa.float32()
        elif isinstance(iceberg_type, DoubleType):
            return pa.float64()
        elif isinstance(iceberg_type, StringType):
            return pa.string()
        elif isinstance(iceberg_type, BinaryType):
            return pa.binary()
        elif isinstance(iceberg_type, TimestampType):
            return pa.timestamp("us")
        elif isinstance(iceberg_type, ListType):
            value_type = self._iceberg_type_to_arrow_type(iceberg_type.element_type)
            return pa.list_(value_type)
        elif isinstance(iceberg_type, StructType):
            fields = []
            for field in iceberg_type.fields:
                fields.append(self._iceberg_field_to_arrow_field(field))
            return pa.struct(fields)
        elif isinstance(iceberg_type, MapType):
            key_type = self._iceberg_type_to_arrow_type(iceberg_type.key_type)
            value_type = self._iceberg_type_to_arrow_type(iceberg_type.value_type)
            return pa.map_(key_type, value_type)
        else:
            # Default to string for unsupported types
            logger.warning(
                f"Unsupported Iceberg type: {iceberg_type}, defaulting to string"
            )
            return pa.string()

    def list_tables(self, pattern: Optional[str] = None) -> List[str]:
        """
        List available tables.

        Args:
            pattern: Optional pattern to filter table names

        Returns:
            List of table names
        """
        tables = []

        # Try to list tables from Iceberg catalog
        if HAS_PYICEBERG and self.catalog:
            try:
                namespace_tables = self.catalog.list_tables(self.namespace)
                tables.extend([t.name for t in namespace_tables])
            except Exception as e:
                logger.warning(f"Failed to list tables from Iceberg catalog: {e}")

        # Look for directories with Parquet files as fallback
        if os.path.exists(self.warehouse_path):
            for item in os.listdir(self.warehouse_path):
                item_path = os.path.join(self.warehouse_path, item)
                if os.path.isdir(item_path):
                    parquet_files = [
                        f for f in os.listdir(item_path) if f.endswith(".parquet")
                    ]
                    if parquet_files and item not in tables:
                        tables.append(item)

        # Filter by pattern if provided
        if pattern:
            import fnmatch

            tables = [t for t in tables if fnmatch.fnmatch(t, pattern)]

        return tables


# Factory function to create the persistence layer
def create_iceberg_persistence(
    base_path: str,
    catalog_name: str = "flare",
    warehouse_path: Optional[str] = None,
    catalog_properties: Optional[Dict[str, str]] = None,
    namespace: str = "default",
) -> IcebergPersistence:
    """
    Create an Iceberg persistence layer.

    Args:
        base_path: Base path for Iceberg data
        catalog_name: Name of the Iceberg catalog
        warehouse_path: Warehouse path for Iceberg catalog (optional)
        catalog_properties: Additional properties for the catalog
        namespace: Default namespace for tables

    Returns:
        IcebergPersistence instance
    """
    return IcebergPersistence(
        base_path=base_path,
        catalog_name=catalog_name,
        warehouse_path=warehouse_path,
        catalog_properties=catalog_properties,
        namespace=namespace,
    )
