import time
import logging
import os
import signal
import sys
from typing import Dict, List, Optional, Tuple, Any

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from pyarrow.flight import FlightDescriptor, Ticket
from cloudpickle import dumps

# Import exchanger base classes from server module
from flight_server import AbstractExchanger, AddExchangeAction

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mallard.demo")


# ================================================================================
# Configuration
# ================================================================================
class ClientConfig:
    """Configuration for Flight client."""

    def __init__(self, location: str, name: str):
        self.location = location
        self.name = name

    def __str__(self) -> str:
        return f"ClientConfig(name={self.name}, location={self.location})"


# Default client configurations
SERVER1_CONFIG = ClientConfig(location="grpc://localhost:8815", name="server1")
SERVER2_CONFIG = ClientConfig(location="grpc://localhost:8816", name="server2")


# ================================================================================
# Client Connection Management
# ================================================================================
class FlightClientManager:
    """
    Manages Flight client connections.
    Follows Single Responsibility Principle by focusing only on client connections.
    """

    def __init__(self, configs: Optional[List[ClientConfig]] = None):
        """Initialize with client configurations."""
        self.configs = configs or [SERVER1_CONFIG, SERVER2_CONFIG]
        self.clients = {}
        self._connect_all()

    def _connect_all(self):
        """Connect to all configured servers."""
        for config in self.configs:
            try:
                self.clients[config.name] = self._connect(config)
                logger.info(f"Connected to {config.name} at {config.location}")
            except Exception as e:
                logger.error(f"Failed to connect to {config.name}: {e}")
                raise

    def _connect(self, config: ClientConfig) -> flight.FlightClient:
        """Connect to a single server."""
        return flight.connect(config.location)

    def get_client(self, name: str) -> flight.FlightClient:
        """Get a client by name."""
        if name not in self.clients:
            raise ValueError(f"Unknown client: {name}")
        return self.clients[name]

    def close_all(self):
        """Close all client connections."""
        for name, client in self.clients.items():
            try:
                client.close()
                logger.info(f"Closed connection to {name}")
            except Exception as e:
                logger.error(f"Error closing connection to {name}: {e}")

        self.clients.clear()


# ================================================================================
# Data Operations
# ================================================================================
class DataOperations:
    """
    Data operations using Flight clients.
    Follows Single Responsibility Principle by focusing on data operations.
    """

    def __init__(self, client_manager: FlightClientManager):
        """Initialize with a client manager."""
        self.client_manager = client_manager

    def execute_query(self, server_name: str, query: str) -> pa.Table:
        """Execute a query on a server."""
        client = self.client_manager.get_client(server_name)
        reader = client.do_get(Ticket(query.encode("utf-8")))
        return reader.read_all()

    def create_table(self, server_name: str, table_name: str, data: pa.Table):
        """Create a table on a server."""
        client = self.client_manager.get_client(server_name)
        descriptor = FlightDescriptor.for_command(table_name.encode())
        writer, _ = client.do_put(descriptor, data.schema)

        for batch in data.to_batches():
            writer.write_batch(batch)

        writer.close()
        logger.info(
            f"Created table {table_name} on {server_name} with {data.num_rows} rows"
        )

    def register_exchanger(self, server_name: str, exchanger_class):
        """Register an exchanger on a server."""
        client = self.client_manager.get_client(server_name)

        # Serialize the exchanger class
        serialized_exchanger = dumps(exchanger_class)

        # Create and send the action
        action = flight.Action(AddExchangeAction.name, serialized_exchanger)
        result = list(client.do_action(action))

        logger.info(f"Registered exchanger {exchanger_class.__name__} on {server_name}")
        return result

    def transfer_table(
        self, from_server: str, to_server: str, table_name: str
    ) -> Tuple[int, float]:
        """Transfer a table from one server to another."""
        # Get the data from the source server
        source_client = self.client_manager.get_client(from_server)
        reader = source_client.do_get(Ticket(f"SELECT * FROM {table_name}".encode()))
        schema = reader.schema

        # Send the data to the destination server
        dest_client = self.client_manager.get_client(to_server)
        descriptor = FlightDescriptor.for_command(table_name.encode())
        writer, _ = dest_client.do_put(descriptor, schema)

        # Track stats
        start_time = time.time()
        total_rows = 0
        batch_count = 0

        # Transfer the data
        for chunk in reader:
            batch = chunk.data
            if batch.num_rows == 0:
                continue

            writer.write_batch(batch)
            batch_count += 1
            total_rows += batch.num_rows

        writer.close()
        duration = time.time() - start_time

        logger.info(
            f"Transferred {total_rows} rows in {batch_count} batches in {duration:.2f} seconds"
        )
        return total_rows, duration

    def exchange_data(self, server_name: str, command: str, data: pa.Table) -> pa.Table:
        """Exchange data with a server using a custom exchanger."""
        client = self.client_manager.get_client(server_name)
        descriptor = FlightDescriptor.for_command(command.encode())

        # Create a writer to send data to the server
        writer, reader = client.do_exchange(descriptor)

        # Track stats
        batch_count = 0
        total_rows = 0

        # Begin writing with the schema
        writer.begin(data.schema)

        # Send the data
        for batch in data.to_batches():
            writer.write_batch(batch)
            batch_count += 1
            total_rows += batch.num_rows

        writer.close()

        # Receive the processed data back
        result_batches = []
        for chunk in reader:
            result_batches.append(chunk.data)

        # Combine into one table
        if result_batches:
            result_table = pa.Table.from_batches(result_batches)
            logger.info(f"Exchanged {total_rows} rows in {batch_count} batches")
            logger.info(f"Received {result_table.num_rows} rows back")
            return result_table
        else:
            logger.warning("No data received from exchange")
            return pa.table({})


# ================================================================================
# Data Generation
# ================================================================================
class DataGenerator:
    """
    Generate sample data for testing.
    Follows Single Responsibility Principle by focusing on data generation.
    """

    @staticmethod
    def create_sample_table() -> pa.Table:
        """Create a small sample table."""
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "value": [10.5, 20.0, 15.5, 30.0, 25.5],
        }
        return pa.Table.from_pydict(data)

    @staticmethod
    def create_flights_table(rows: int = 10000) -> pa.Table:
        """Create a flights table with the specified number of rows."""
        # Create the data
        data = {
            "flight_id": list(range(1, rows + 1)),
            "flight_number": [f"Flight-{i}" for i in range(1, rows + 1)],
            "origin": ["JFK", "LAX", "ORD", "DFW", "SFO"] * (rows // 5 + 1),
            "destination": ["SFO", "JFK", "LAX", "ORD", "DFW"] * (rows // 5 + 1),
            "departure_time": [
                f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d} {(i % 24):02d}:00:00"
                for i in range(rows)
            ],
            "passengers": [50 + i % 200 for i in range(rows)],
        }

        return pa.Table.from_pydict(data)

    @staticmethod
    def load_or_create_parquet(filepath: str, rows: int = 10000) -> pa.Table:
        """Load a parquet file or create it if it doesn't exist."""
        if os.path.exists(filepath):
            logger.info(f"Loading existing data from {filepath}")
            conn = duckdb.connect()
            return conn.sql(f"SELECT * FROM '{filepath}'").fetch_arrow_table()

        # Create the directory if it doesn't exist
        os.makedirs(os.path.dirname(filepath), exist_ok=True)

        # Create a new dataset
        logger.info(f"Creating new dataset with {rows} rows")
        flights_table = DataGenerator.create_flights_table(rows)

        # Save to parquet
        conn = duckdb.connect()
        conn.register("flights_temp", flights_table)
        conn.execute(f"COPY flights_temp TO '{filepath}' (FORMAT PARQUET)")

        logger.info(f"Saved dataset to {filepath}")
        return flights_table


# ================================================================================
# Custom Exchangers
# ================================================================================
class MyStreamingExchanger(AbstractExchanger):
    """
    A custom exchanger that adds a 'processed' column.
    Follows Single Responsibility Principle by focusing on one transformation.
    """

    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        """Process incoming data and add a 'processed' column."""
        start_time = time.time()
        total_rows = 0
        batch_count = 0

        logger.info("Processing data in MyStreamingExchanger")

        # Collect all incoming data
        all_incoming = []
        while True:
            try:
                chunk = reader.read_chunk()
                if chunk.data.num_rows == 0:
                    break
                all_incoming.append(chunk.data)
                total_rows += chunk.data.num_rows
                batch_count += 1
            except StopIteration:
                break

        if not all_incoming:
            logger.info("No data received in exchanger")
            writer.begin(pa.schema([]))
            writer.close()
            return

        # Combine into one Arrow table
        table_in = pa.Table.from_batches(all_incoming)

        # Add the processed column
        processed_col = pa.array([True] * table_in.num_rows, pa.bool_())
        table_out = table_in.append_column("processed", processed_col)

        # Stream the result back
        writer.begin(table_out.schema)
        for batch in table_out.to_batches():
            writer.write_batch(batch)

        writer.close()
        duration = time.time() - start_time

        logger.info(f"Processed {total_rows} rows in {duration:.2f} seconds")


# ================================================================================
# Benchmark Operations
# ================================================================================
class Benchmarker:
    """
    Benchmark various data operations.
    Follows Single Responsibility Principle by focusing on benchmarking.
    """

    def __init__(self, data_ops: DataOperations):
        """Initialize with a data operations instance."""
        self.data_ops = data_ops

    def benchmark_get(self, server_name: str, query: str) -> Dict[str, Any]:
        """Benchmark a GET operation."""
        logger.info(f"Benchmarking GET on {server_name}: {query}")

        start_time = time.time()
        result = self.data_ops.execute_query(server_name, query)
        duration = time.time() - start_time

        metrics = {
            "rows": result.num_rows,
            "duration": duration,
            "throughput": result.num_rows / duration if duration > 0 else 0,
        }

        logger.info(f"GET: {metrics['rows']} rows in {metrics['duration']:.2f} seconds")
        logger.info(f"Throughput: {metrics['throughput']:.0f} rows/second")

        return metrics

    def benchmark_transfer(
        self, from_server: str, to_server: str, table_name: str
    ) -> Dict[str, Any]:
        """Benchmark a data transfer between servers."""
        logger.info(f"Benchmarking transfer {from_server} → {to_server}: {table_name}")

        start_time = time.time()
        rows, transfer_time = self.data_ops.transfer_table(
            from_server, to_server, table_name
        )
        total_time = time.time() - start_time

        # Verify the transfer
        dest_rows = (
            self.data_ops.execute_query(to_server, f"SELECT COUNT(*) FROM {table_name}")
            .to_pandas()
            .iloc[0, 0]
        )

        metrics = {
            "rows": rows,
            "transfer_time": transfer_time,
            "total_time": total_time,
            "throughput": rows / total_time if total_time > 0 else 0,
            "verified_rows": dest_rows,
        }

        logger.info(
            f"Transfer: {metrics['rows']} rows in {metrics['total_time']:.2f} seconds"
        )
        logger.info(f"Throughput: {metrics['throughput']:.0f} rows/second")

        return metrics

    def benchmark_exchange(
        self, server_name: str, command: str, data: pa.Table
    ) -> Dict[str, Any]:
        """Benchmark a custom exchange operation."""
        logger.info(f"Benchmarking exchange on {server_name}: {command}")

        start_time = time.time()
        result = self.data_ops.exchange_data(server_name, command, data)
        duration = time.time() - start_time

        metrics = {
            "input_rows": data.num_rows,
            "output_rows": result.num_rows,
            "duration": duration,
            "throughput": data.num_rows / duration if duration > 0 else 0,
        }

        # Verify the processed column
        has_processed = "processed" in result.column_names
        all_processed = False
        if has_processed:
            all_processed = all(result.column("processed").to_pylist())

        metrics["has_processed_column"] = has_processed
        metrics["all_processed"] = all_processed

        logger.info(
            f"Exchange: {metrics['input_rows']} rows in {metrics['duration']:.2f} seconds"
        )
        logger.info(f"Throughput: {metrics['throughput']:.0f} rows/second")
        logger.info(f"Processed column: {has_processed}, All True: {all_processed}")

        return metrics


# ================================================================================
# Demo Runner
# ================================================================================
class DemoRunner:
    """
    Runs a complete demonstration of Flight capabilities.
    Follows Single Responsibility Principle by focusing on orchestration.
    """

    def __init__(self):
        """Initialize the demo."""
        self.client_manager = FlightClientManager()
        self.data_ops = DataOperations(self.client_manager)
        self.benchmarker = Benchmarker(self.data_ops)

        # Register signal handlers for clean shutdown
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _handle_interrupt(self, signum, frame):
        """Handle interrupt signals."""
        logger.info("Interrupt received, shutting down...")
        self.cleanup()
        sys.exit(0)

    def setup(self):
        """Set up the demo environment."""
        logger.info("Setting up demo environment...")

        # Wait for servers
        self._wait_for_servers()

        # Create sample data
        self._setup_sample_data()

        # Register custom exchanger
        self._register_custom_exchanger()

    def _wait_for_servers(self, max_attempts: int = 30):
        """Wait for servers to be ready."""
        logger.info("Waiting for servers to be ready...")

        for attempt in range(max_attempts):
            try:
                # Try a simple query to verify connection
                for server in ["server1", "server2"]:
                    result = self.data_ops.execute_query(server, "SELECT 1")
                    if result.num_rows != 1:
                        raise ValueError(f"Unexpected result from {server}")

                logger.info("All servers are ready")
                return True
            except Exception as e:
                logger.warning(
                    f"Servers not ready yet (attempt {attempt+1}/{max_attempts}): {e}"
                )
                time.sleep(1)

        raise RuntimeError(f"Servers not ready after {max_attempts} attempts")

    def _setup_sample_data(self):
        """Set up sample data for the demo."""
        # Create a simple table on server1
        simple_table = DataGenerator.create_sample_table()
        self.data_ops.create_table("server1", "simple_table", simple_table)

        # Load or create flights data
        data_dir = os.path.join(
            os.path.dirname(os.path.abspath(__file__)), "..", "data"
        )
        parquet_path = os.path.join(data_dir, "flights.parquet")
        self.flights_table = DataGenerator.load_or_create_parquet(parquet_path)

        # Create flights table on server1
        self.data_ops.create_table("server1", "flights", self.flights_table)

    def _register_custom_exchanger(self):
        """Register the custom exchanger."""
        logger.info("Registering custom exchanger...")
        try:
            self.data_ops.register_exchanger("server1", MyStreamingExchanger)
            logger.info("Custom exchanger registered successfully")
        except Exception as e:
            logger.error(f"Failed to register custom exchanger: {e}")

    def run_simple_demo(self):
        """Run a simple demonstration."""
        logger.info("\n" + "=" * 80)
        logger.info("SIMPLE DEMONSTRATION")
        logger.info("=" * 80)

        # Query the simple table
        result = self.data_ops.execute_query("server1", "SELECT * FROM simple_table")
        logger.info(f"Simple table contents:\n{result.to_pandas()}")

        # Transfer the table to server2
        self.data_ops.transfer_table("server1", "server2", "simple_table")

        # Verify the transfer
        result = self.data_ops.execute_query("server2", "SELECT * FROM simple_table")
        logger.info(f"Table transferred to server2:\n{result.to_pandas()}")

    def run_benchmarks(self):
        """Run benchmarks."""
        logger.info("\n" + "=" * 80)
        logger.info("RUNNING BENCHMARKS")
        logger.info("=" * 80)

        # Benchmark GET
        logger.info("\n--- Benchmark GET ---")
        get_metrics = self.benchmarker.benchmark_get("server1", "SELECT * FROM flights")

        # Benchmark Transfer
        logger.info("\n--- Benchmark Transfer ---")
        transfer_metrics = self.benchmarker.benchmark_transfer(
            "server1", "server2", "flights"
        )

        # Benchmark Custom Exchange
        logger.info("\n--- Benchmark Custom Exchange ---")
        exchange_metrics = self.benchmarker.benchmark_exchange(
            "server1", "my_streaming_exchanger", self.flights_table
        )

        # Print summary
        logger.info("\n" + "=" * 80)
        logger.info("BENCHMARK SUMMARY")
        logger.info("=" * 80)

        logger.info(f"\nTotal rows: {self.flights_table.num_rows:,}")
        logger.info(
            f"GET time: {get_metrics['duration']:.2f} seconds ({get_metrics['throughput']:,.0f} rows/second)"
        )
        logger.info(
            f"Transfer time: {transfer_metrics['transfer_time']:.2f} seconds ({transfer_metrics['throughput']:,.0f} rows/second)"
        )
        logger.info(
            f"Exchange time: {exchange_metrics['duration']:.2f} seconds ({exchange_metrics['throughput']:,.0f} rows/second)"
        )

    def cleanup(self):
        """Clean up resources."""
        logger.info("Cleaning up resources...")
        self.client_manager.close_all()

    def run(self):
        """Run the complete demo."""
        try:
            self.setup()
            self.run_simple_demo()
            self.run_benchmarks()
            logger.info("\nDemo completed successfully!")
            return True
        except Exception as e:
            logger.error(f"Demo failed: {e}")
            import traceback

            traceback.print_exc()
            return False
        finally:
            self.cleanup()


# ================================================================================
# Main Entry Point
# ================================================================================
def main():
    """Main entry point for the demo."""
    try:
        demo = DemoRunner()
        success = demo.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("\nDemo interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}")
        import traceback

        traceback.print_exc()
        sys.exit(1)


if __name__ == "__main__":
    main()
