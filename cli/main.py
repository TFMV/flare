import logging
import sys
from typing import List, Optional
import typer
from rich.console import Console
from rich.logging import RichHandler
import pyarrow as pa
import pyarrow.flight as flight

from flare.config.settings import FlareConfig, LogLevel, SchemaFormat
from flare.flight.server import serve as serve_flight

# Set up Typer app
app = typer.Typer(help="Flare: Apache Arrow Flight gateway for Kafka")
console = Console()

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(message)s",
    handlers=[RichHandler(rich_tracebacks=True)],
)

logger = logging.getLogger("flare")


@app.command()
def serve(
    source: str = typer.Option(
        "localhost:9092", "--source", "-s", help="Kafka bootstrap servers"
    ),
    schema: Optional[str] = typer.Option(None, "--schema", help="Schema registry URL"),
    host: str = typer.Option("0.0.0.0", "--host", "-h", help="Flight server host"),
    port: int = typer.Option(8815, "--port", "-p", help="Flight server port"),
    ttl: int = typer.Option(300, "--ttl", help="Time-to-live for streams in seconds"),
    batch_size: int = typer.Option(
        1000, "--batch-size", help="Maximum messages per Arrow batch"
    ),
    topics: Optional[List[str]] = typer.Option(
        None, "--topic", help="Topics to consume (multiple allowed)"
    ),
    topic_filter: Optional[str] = typer.Option(
        None, "--filter", help="Topic regex filter"
    ),
    schema_format: str = typer.Option(
        "infer", "--schema-format", help="Schema format (avro, protobuf, json, infer)"
    ),
    log_level: str = typer.Option(
        "INFO", "--log-level", help="Log level (DEBUG, INFO, WARNING, ERROR, CRITICAL)"
    ),
    metrics: bool = typer.Option(
        True, "--metrics/--no-metrics", help="Enable Prometheus metrics"
    ),
    metrics_port: int = typer.Option(
        8816, "--metrics-port", help="Prometheus metrics port"
    ),
    persist: bool = typer.Option(
        False, "--persist", help="Enable persistence of batches"
    ),
    persist_path: Optional[str] = typer.Option(
        None, "--persist-path", help="Path for persistence"
    ),
):
    """Start the Flare Flight server for Kafka."""
    try:
        # Create config
        config = FlareConfig()
        config.kafka_bootstrap_servers = source
        config.schema_registry_url = schema
        config.flight_host = host
        config.flight_port = port
        config.stream_ttl = ttl
        config.batch_size = batch_size
        config.topics = topics
        config.topic_filter_regex = topic_filter
        config.schema_format = SchemaFormat(schema_format)
        config.log_level = LogLevel(log_level)
        config.enable_metrics = metrics
        config.metrics_port = metrics_port
        config.enable_persistence = persist
        config.persistence_path = persist_path

        # Configure logging
        config.setup_logging()

        # Display configuration
        console.print(
            f"[bold green]Starting Flare server with configuration:[/bold green]"
        )
        console.print(
            f"  Kafka bootstrap servers: [cyan]{config.kafka_bootstrap_servers}[/cyan]"
        )
        console.print(
            f"  Flight server: [cyan]{config.flight_host}:{config.flight_port}[/cyan]"
        )
        if config.topics:
            console.print(f"  Topics: [cyan]{', '.join(config.topics)}[/cyan]")
        if config.topic_filter_regex:
            console.print(f"  Topic filter: [cyan]{config.topic_filter_regex}[/cyan]")
        console.print(f"  Stream TTL: [cyan]{config.stream_ttl}s[/cyan]")
        console.print(f"  Batch size: [cyan]{config.batch_size}[/cyan]")
        if config.schema_registry_url:
            console.print(
                f"  Schema registry: [cyan]{config.schema_registry_url}[/cyan]"
            )
        console.print(f"  Schema format: [cyan]{config.schema_format.value}[/cyan]")
        console.print(f"  Metrics enabled: [cyan]{config.enable_metrics}[/cyan]")
        if config.enable_metrics:
            console.print(f"  Metrics port: [cyan]{config.metrics_port}[/cyan]")
        console.print(
            f"  Persistence enabled: [cyan]{config.enable_persistence}[/cyan]"
        )
        if config.enable_persistence and config.persistence_path:
            console.print(f"  Persistence path: [cyan]{config.persistence_path}[/cyan]")

        # Start the server
        serve_flight(
            host=config.flight_host,
            port=config.flight_port,
            kafka_bootstrap_servers=config.kafka_bootstrap_servers,
            schema_registry_url=config.schema_registry_url,
            topics=config.topics,
            ttl=config.stream_ttl,
            batch_size=config.batch_size,
            enable_metrics=config.enable_metrics,
            metrics_port=config.metrics_port,
        )

    except Exception as e:
        console.print(f"[bold red]Error starting Flare server:[/bold red] {str(e)}")
        logger.exception("Failed to start Flare server")
        sys.exit(1)


@app.command()
def test_client(
    host: str = typer.Option("localhost", "--host", "-h", help="Flight server host"),
    port: int = typer.Option(8815, "--port", "-p", help="Flight server port"),
    topic: str = typer.Option(..., "--topic", "-t", help="Topic to fetch"),
    limit: int = typer.Option(10, "--limit", "-l", help="Maximum records to display"),
    quiet: bool = typer.Option(False, "--quiet", "-q", help="Suppress detailed output"),
):
    """Test client to fetch and print from a topic stream."""
    try:
        # Connect to the Flight server
        location = f"grpc://{host}:{port}"
        client = flight.FlightClient(location)

        console.print(
            f"[bold green]Connecting to Flare server at {location}[/bold green]"
        )

        # Get the flights (topics)
        if not quiet:
            console.print("[bold cyan]Available topics:[/bold cyan]")
            for flight_info in client.list_flights():
                descriptor = flight_info.descriptor
                if descriptor.type == flight.DescriptorType.CMD:
                    topic_name = descriptor.command.decode("utf-8")
                    console.print(f"  - {topic_name}")

        # Get the flight descriptor for the requested topic
        descriptor = flight.FlightDescriptor.for_command(topic.encode())

        try:
            # Get the flight info
            flight_info = client.get_flight_info(descriptor)

            # Get the ticket from the first endpoint
            if not flight_info.endpoints:
                console.print(
                    f"[bold red]No endpoints found for topic {topic}[/bold red]"
                )
                return

            ticket = flight_info.endpoints[0].ticket

            # Do get to retrieve the data
            console.print(f"[bold green]Retrieving data for topic {topic}[/bold green]")

            reader = client.do_get(ticket)

            # Read and display the batches
            record_count = 0
            for batch, _ in reader:
                # Display batch schema if not quiet
                if not quiet and record_count == 0:
                    console.print("[bold cyan]Schema:[/bold cyan]")
                    console.print(batch.schema)

                # Display records
                for i in range(min(batch.num_rows, limit - record_count)):
                    if record_count >= limit:
                        break

                    # Convert row to dict for display
                    row = {}
                    for j, field in enumerate(batch.schema.names):
                        row[field] = batch.column(j)[i].as_py()

                    console.print(f"[cyan]Record {record_count}:[/cyan] {row}")
                    record_count += 1

                if record_count >= limit:
                    break

            console.print(
                f"[bold green]Displayed {record_count} records from topic {topic}[/bold green]"
            )

        except flight.FlightUnavailableError:
            console.print(
                f"[bold red]Topic {topic} not found or not available[/bold red]"
            )

    except Exception as e:
        console.print(f"[bold red]Error in test client:[/bold red] {str(e)}")
        logger.exception("Test client error")
        sys.exit(1)


@app.command()
def duckdb_example(
    host: str = typer.Option("localhost", "--host", "-h", help="Flight server host"),
    port: int = typer.Option(8815, "--port", "-p", help="Flight server port"),
    topic: str = typer.Option(..., "--topic", "-t", help="Topic to query"),
    query: str = typer.Option(
        "SELECT * FROM topic_table LIMIT 10",
        "--query",
        "-q",
        help="SQL query to execute",
    ),
):
    """Run a DuckDB query against a Flare stream."""
    try:
        import duckdb

        # Connect to the Flight server
        location = f"grpc://{host}:{port}"
        console.print(
            f"[bold green]Connecting to Flare server at {location}[/bold green]"
        )

        # Create the DuckDB connection
        conn = duckdb.connect(":memory:")

        # Execute the example
        console.print(f"[bold green]Running DuckDB query on topic {topic}[/bold green]")

        # Register the Flight stream as a DuckDB table
        register_query = f"""
        INSTALL arrow;
        LOAD arrow;
        CALL arrow_flight_register_endpoint(
            '{location}', 
            flight_command=>'{topic}',
            table_name=>'topic_table'
        );
        """
        conn.execute(register_query)

        # Execute the user's query
        console.print(f"[bold cyan]Query:[/bold cyan] {query}")
        result = conn.execute(query).fetchall()

        # Display the results
        if result:
            # Get column names from the query
            col_names = conn.execute(f"SELECT * FROM topic_table WHERE 1=0").description
            headers = [col[0] for col in col_names]

            # Print results as a table
            console.print("[bold green]Results:[/bold green]")
            for row in result:
                row_dict = {headers[i]: val for i, val in enumerate(row)}
                console.print(row_dict)
        else:
            console.print("[yellow]No results returned from query[/yellow]")

    except ImportError:
        console.print(
            "[bold red]Error: DuckDB is required for this example.[/bold red]"
        )
        console.print("Install it with: pip install duckdb")
        sys.exit(1)
    except Exception as e:
        console.print(f"[bold red]Error in DuckDB example:[/bold red] {str(e)}")
        logger.exception("DuckDB example error")
        sys.exit(1)


if __name__ == "__main__":
    app()
