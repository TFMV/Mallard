import time
import logging
import os
import signal
import sys
import warnings
from typing import Dict, List, Optional, Tuple, Any

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from pyarrow.flight import FlightDescriptor, Ticket
from cloudpickle import dumps

# Filter out Arrow alignment warnings
warnings.filterwarnings("ignore", message="An input buffer was poorly aligned")

# Import core types from the Flight server module
from flight_server import (
    AbstractExchanger,
    AddExchangeAction,
    FlightServerManager,
    FlightServerConfig,
)

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mallard.demo")


# ------------------------------------------------------------------------------
# Client Configuration and Connection Manager
# ------------------------------------------------------------------------------
class ClientConfig:
    """Holds configuration for a Flight client."""

    def __init__(self, location: str, name: str):
        self.location = location
        self.name = name

    def __str__(self) -> str:
        return f"ClientConfig(name={self.name}, location={self.location})"


# Default client configurations for two servers
SERVER1_CONFIG = ClientConfig(location="grpc://localhost:8815", name="server1")
SERVER2_CONFIG = ClientConfig(location="grpc://localhost:8816", name="server2")


class FlightClientManager:
    """
    Manages Flight client connections.
    This class is responsible solely for connecting and disconnecting clients.
    """

    def __init__(self, configs: Optional[List[ClientConfig]] = None):
        self.configs = configs or [SERVER1_CONFIG, SERVER2_CONFIG]
        self.clients: Dict[str, flight.FlightClient] = {}
        self._connect_all()

    def _connect_all(self):
        for config in self.configs:
            try:
                self.clients[config.name] = flight.connect(config.location)
                logger.info(f"Connected to {config.name} at {config.location}")
            except Exception as e:
                logger.error(f"Failed to connect to {config.name}: {e}")
                raise

    def get_client(self, name: str) -> flight.FlightClient:
        if name not in self.clients:
            raise ValueError(f"Unknown client: {name}")
        return self.clients[name]

    def close_all(self):
        for name, client in self.clients.items():
            try:
                client.close()
                logger.info(f"Closed connection to {name}")
            except Exception as e:
                logger.error(f"Error closing connection to {name}: {e}")
        self.clients.clear()


# ------------------------------------------------------------------------------
# Data Operations (Client-Side)
# ------------------------------------------------------------------------------
class DataOperations:
    """
    Provides methods for executing queries, transferring data, and performing exchanges.
    This class is solely responsible for client data operations.
    """

    def __init__(self, client_manager: FlightClientManager):
        self.client_manager = client_manager

    def execute_query(self, server_name: str, query: str) -> pa.Table:
        client = self.client_manager.get_client(server_name)
        reader = client.do_get(Ticket(query.encode("utf-8")))
        return reader.read_all()

    def create_table(self, server_name: str, table_name: str, data: pa.Table):
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
        client = self.client_manager.get_client(server_name)
        serialized_exchanger = dumps(exchanger_class)
        action = flight.Action(AddExchangeAction.name, serialized_exchanger)
        result = list(client.do_action(action))
        logger.info(f"Registered exchanger {exchanger_class.__name__} on {server_name}")
        return result

    def transfer_table(
        self, from_server: str, to_server: str, table_name: str
    ) -> Tuple[int, float]:
        source_client = self.client_manager.get_client(from_server)
        reader = source_client.do_get(Ticket(f"SELECT * FROM {table_name}".encode()))
        schema = reader.schema
        dest_client = self.client_manager.get_client(to_server)
        descriptor = FlightDescriptor.for_command(table_name.encode())
        writer, _ = dest_client.do_put(descriptor, schema)
        start_time = time.time()
        total_rows = 0
        batch_count = 0
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
            f"Transferred {total_rows} rows in {batch_count} batches in {duration*1000:.2f} ms"
        )
        return total_rows, duration

    def exchange_data(self, server_name: str, command: str, data: pa.Table) -> pa.Table:
        client = self.client_manager.get_client(server_name)
        descriptor = FlightDescriptor.for_command(command.encode())
        writer, reader = client.do_exchange(descriptor)
        writer.begin(data.schema)
        batch_count = 0
        total_rows = 0
        for batch in data.to_batches():
            writer.write_batch(batch)
            batch_count += 1
            total_rows += batch.num_rows
        writer.close()
        result_batches = []
        for chunk in reader:
            result_batches.append(chunk.data)
        if result_batches:
            result_table = pa.Table.from_batches(result_batches)
            logger.info(f"Exchanged {total_rows} rows in {batch_count} batches")
            logger.info(f"Received {result_table.num_rows} rows back")
            return result_table
        else:
            logger.warning("No data received from exchange")
            return pa.table({})


# ------------------------------------------------------------------------------
# Data Generation
# ------------------------------------------------------------------------------
class DataGenerator:
    """Responsible solely for generating sample data."""

    @staticmethod
    def create_sample_table() -> pa.Table:
        data = {
            "id": [1, 2, 3, 4, 5],
            "name": ["Alice", "Bob", "Charlie", "Dave", "Eve"],
            "value": [10.5, 20.0, 15.5, 30.0, 25.5],
        }
        return pa.Table.from_pydict(data)

    @staticmethod
    def create_flights_table(rows: int = 10000) -> pa.Table:
        base_origins = ["JFK", "LAX", "ORD", "DFW", "SFO"]
        base_destinations = ["SFO", "JFK", "LAX", "ORD", "DFW"]

        data = {
            "flight_id": list(range(1, rows + 1)),
            "flight_number": [f"Flight-{i}" for i in range(1, rows + 1)],
            "origin": (base_origins * ((rows // len(base_origins)) + 1))[:rows],
            "destination": (base_destinations * ((rows // len(base_destinations)) + 1))[
                :rows
            ],
            "departure_time": [
                f"2023-{(i % 12) + 1:02d}-{(i % 28) + 1:02d} {(i % 24):02d}:00:00"
                for i in range(rows)
            ],
            "passengers": [50 + i % 200 for i in range(rows)],
        }
        return pa.Table.from_pydict(data)

    @staticmethod
    def load_or_create_parquet(
        filepath: str, rows: int = 10000, limit_rows: Optional[int] = None
    ) -> pa.Table:
        if os.path.exists(filepath):
            logger.info(f"Loading existing data from {filepath}")
            conn = duckdb.connect()
            if limit_rows:
                logger.info(f"Limiting to {limit_rows} rows for benchmarking")
                return conn.sql(
                    f"SELECT * FROM '{filepath}' LIMIT {limit_rows}"
                ).fetch_arrow_table()
            else:
                logger.info(f"Loading all rows from {filepath}")
                return conn.sql(f"SELECT * FROM '{filepath}'").fetch_arrow_table()
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        logger.info(f"Creating new dataset with {rows} rows")
        flights_table = DataGenerator.create_flights_table(rows)
        conn = duckdb.connect()
        conn.register("flights_temp", flights_table)
        conn.execute(f"COPY flights_temp TO '{filepath}' (FORMAT PARQUET)")
        logger.info(f"Saved dataset to {filepath}")
        return flights_table


# ------------------------------------------------------------------------------
# Custom Exchanger for the Demo
# ------------------------------------------------------------------------------
class CustomStreamingExchanger(AbstractExchanger):
    """
    Custom exchanger that mirrors the default behavior.
    This is used to demonstrate registering custom logic from the client.
    """

    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        start_time = time.time()
        total_rows = 0
        batch_count = 0
        logger.info("Processing data in CustomStreamingExchanger")
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
        table_in = pa.Table.from_batches(all_incoming)
        processed_col = pa.array([True] * table_in.num_rows, pa.bool_())
        table_out = table_in.append_column("processed", processed_col)
        writer.begin(table_out.schema)
        for batch in table_out.to_batches():
            writer.write_batch(batch)
        writer.close()
        duration = time.time() - start_time
        logger.info(f"Processed {total_rows} rows in {duration*1000:.2f} ms")


# ------------------------------------------------------------------------------
# Benchmark Operations
# ------------------------------------------------------------------------------
class Benchmarker:
    """
    Benchmarks various data operations.
    This class is solely responsible for gathering and logging performance metrics.
    """

    def __init__(self, data_ops: DataOperations):
        self.data_ops = data_ops
        self.metrics = {}

    def benchmark_get(self, server_name: str, query: str) -> Dict[str, Any]:
        logger.info(f"Benchmarking GET on {server_name}: {query}")
        start_time = time.time()
        result = self.data_ops.execute_query(server_name, query)
        duration = time.time() - start_time
        metrics = {
            "rows": result.num_rows,
            "duration": duration,
            "throughput": result.num_rows / duration if duration > 0 else 0,
        }
        logger.info(f"GET: {metrics['rows']} rows in {metrics['duration']*1000:.2f} ms")
        logger.info(f"Throughput: {metrics['throughput']:,.0f} rows/second")
        self.metrics["get"] = metrics
        return metrics

    def benchmark_transfer(
        self, from_server: str, to_server: str, table_name: str
    ) -> Dict[str, Any]:
        logger.info(f"Benchmarking transfer {from_server} → {to_server}: {table_name}")
        start_time = time.time()
        rows, transfer_time = self.data_ops.transfer_table(
            from_server, to_server, table_name
        )
        total_time = time.time() - start_time
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
            f"Transfer: {metrics['rows']} rows in {metrics['total_time']*1000:.2f} ms"
        )
        logger.info(f"Throughput: {metrics['throughput']:,.0f} rows/second")
        self.metrics["transfer"] = metrics
        return metrics

    def benchmark_exchange(
        self, server_name: str, command: str, data: pa.Table
    ) -> Dict[str, Any]:
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
        has_processed = "processed" in result.column_names
        all_processed = False
        if has_processed:
            all_processed = all(result.column("processed").to_pylist())
        metrics["has_processed_column"] = has_processed
        metrics["all_processed"] = all_processed
        logger.info(
            f"Exchange: {metrics['input_rows']} rows in {metrics['duration']*1000:.2f} ms"
        )
        logger.info(f"Throughput: {metrics['throughput']:,.0f} rows/second")
        logger.info(f"Processed column: {has_processed}, All True: {all_processed}")
        self.metrics["exchange"] = metrics
        return metrics

    def print_formatted_report(self, total_rows: int):
        """Prints a nicely formatted benchmark report with colors and formatting."""
        # ANSI color codes
        RESET = "\033[0m"
        BOLD = "\033[1m"
        GREEN = "\033[32m"
        BLUE = "\033[34m"
        CYAN = "\033[36m"
        YELLOW = "\033[33m"

        # Header
        print(f"\n{BOLD}{'=' * 80}{RESET}")
        print(f"{BOLD}{BLUE}🦆 MALLARD BENCHMARK REPORT 🦆{RESET}")
        print(f"{BOLD}{'=' * 80}{RESET}")

        # Dataset info
        print(f"\n{BOLD}📊 Dataset Information:{RESET}")
        print(f"  • Total rows: {CYAN}{total_rows:,}{RESET}")

        # Performance metrics
        print(f"\n{BOLD}⚡ Performance Metrics:{RESET}")

        if "get" in self.metrics:
            get = self.metrics["get"]
            print(f"  • {BOLD}GET Operation:{RESET}")
            print(f"    - Duration: {CYAN}{get['duration']*1000:.2f} ms{RESET}")
            print(
                f"    - Throughput: {GREEN}{get['throughput']:,.0f} rows/second{RESET}"
            )

        if "transfer" in self.metrics:
            transfer = self.metrics["transfer"]
            print(f"  • {BOLD}TRANSFER Operation:{RESET}")
            print(
                f"    - Duration: {CYAN}{transfer['transfer_time']*1000:.2f} ms{RESET}"
            )
            print(
                f"    - Throughput: {GREEN}{transfer['throughput']:,.0f} rows/second{RESET}"
            )
            print(f"    - Verified rows: {YELLOW}{transfer['verified_rows']:,}{RESET}")

        if "exchange" in self.metrics:
            exchange = self.metrics["exchange"]
            print(f"  • {BOLD}EXCHANGE Operation:{RESET}")
            print(f"    - Duration: {CYAN}{exchange['duration']*1000:.2f} ms{RESET}")
            print(
                f"    - Throughput: {GREEN}{exchange['throughput']:,.0f} rows/second{RESET}"
            )
            print(
                f"    - Processed column: {YELLOW}{exchange['has_processed_column']}{RESET}"
            )

        # Footer
        print(f"\n{BOLD}{'=' * 80}{RESET}")
        print(f"{BOLD}{BLUE}🚀 MALLARD - High-Performance Data Exchange{RESET}")
        print(f"{BOLD}{'=' * 80}{RESET}\n")


# ------------------------------------------------------------------------------
# Demo Runner
# ------------------------------------------------------------------------------
class DemoRunner:
    """
    Orchestrates the demo: it sets up data, registers custom exchangers,
    runs simple operations and benchmarks.
    """

    def __init__(self):
        self.client_manager = FlightClientManager()
        self.data_ops = DataOperations(self.client_manager)
        self.benchmarker = Benchmarker(self.data_ops)
        signal.signal(signal.SIGINT, self._handle_interrupt)
        signal.signal(signal.SIGTERM, self._handle_interrupt)

    def _handle_interrupt(self, signum, frame):
        logger.info("Interrupt received, shutting down...")
        self.cleanup()
        sys.exit(0)

    def setup(self):
        logger.info("Setting up demo environment...")
        self._wait_for_servers()
        self._setup_sample_data()
        self._register_custom_exchanger()

    def _wait_for_servers(self, max_attempts: int = 30):
        logger.info("Waiting for servers to be ready...")
        for attempt in range(max_attempts):
            try:
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
        simple_table = DataGenerator.create_sample_table()
        self.data_ops.create_table("server1", "simple_table", simple_table)

        # Use the correct path to the flights.parquet file with 24 million rows
        data_dir = os.path.join(
            os.path.dirname(os.path.dirname(os.path.abspath(__file__))), "data"
        )
        os.makedirs(data_dir, exist_ok=True)
        parquet_path = os.path.join(data_dir, "flights.parquet")

        if os.path.exists(parquet_path):
            logger.info(f"Using existing flights dataset at {parquet_path}")
            self.flights_table = DataGenerator.load_or_create_parquet(parquet_path)
            row_count = self.flights_table.num_rows
            logger.info(f"Loaded flights dataset with {row_count:,} rows")
            self.data_ops.create_table("server1", "flights", self.flights_table)
        else:
            # Fallback to the default location
            logger.warning(
                f"Could not find flights dataset at {parquet_path}, using default location"
            )
            default_data_dir = os.path.join(
                os.path.dirname(os.path.abspath(__file__)), "data"
            )
            os.makedirs(default_data_dir, exist_ok=True)
            default_parquet_path = os.path.join(default_data_dir, "flights.parquet")
            self.flights_table = DataGenerator.load_or_create_parquet(
                default_parquet_path
            )
            row_count = self.flights_table.num_rows
            logger.info(f"Loaded flights dataset with {row_count:,} rows")
            self.data_ops.create_table("server1", "flights", self.flights_table)

    def _register_custom_exchanger(self):
        logger.info("Registering custom exchanger...")
        try:
            self.data_ops.register_exchanger("server1", CustomStreamingExchanger)
            logger.info("Custom exchanger registered successfully")
        except Exception as e:
            logger.error(f"Failed to register custom exchanger: {e}")

    def run_simple_demo(self):
        logger.info("\n" + "=" * 80)
        logger.info("SIMPLE DEMONSTRATION")
        logger.info("=" * 80)

        result = self.data_ops.execute_query("server1", "SELECT * FROM simple_table")
        logger.info(f"Simple table contents:\n{result.to_pandas()}")

        self.data_ops.transfer_table("server1", "server2", "simple_table")

        result = self.data_ops.execute_query("server2", "SELECT * FROM simple_table")
        logger.info(f"Table transferred to server2:\n{result.to_pandas()}")

    def run_benchmarks(self):
        logger.info("\n" + "=" * 80)
        logger.info("RUNNING BENCHMARKS")
        logger.info("=" * 80)

        logger.info("\n--- Benchmark GET ---")
        get_metrics = self.benchmarker.benchmark_get("server1", "SELECT * FROM flights")

        logger.info("\n--- Benchmark Transfer ---")
        transfer_metrics = self.benchmarker.benchmark_transfer(
            "server1", "server2", "flights"
        )

        logger.info("\n--- Benchmark Custom Exchange ---")
        exchange_metrics = self.benchmarker.benchmark_exchange(
            "server1", "my_streaming_exchanger", self.flights_table
        )

        # Print the formatted report instead of the simple summary
        self.benchmarker.print_formatted_report(self.flights_table.num_rows)

    def cleanup(self):
        logger.info("Cleaning up resources...")
        self.client_manager.close_all()

    def run(self):
        try:
            self.setup()
            self.run_simple_demo()
            self.run_benchmarks()
            logger.info("\nDemo completed successfully!")
            return True
        except Exception as e:
            logger.error(f"Demo failed: {e}", exc_info=True)
            return False
        finally:
            self.cleanup()


# ------------------------------------------------------------------------------
# Main Entry Point for the Demo
# ------------------------------------------------------------------------------
def main():
    # Start two Flight server instances via the server manager.
    server_configs = [
        FlightServerConfig(location="grpc://localhost:8815", server_id=":memory:"),
        FlightServerConfig(location="grpc://localhost:8816", server_id=":memory:"),
    ]
    server_manager = FlightServerManager(server_configs)
    try:
        server_manager.start_servers()
        demo = DemoRunner()
        success = demo.run()
        sys.exit(0 if success else 1)
    except KeyboardInterrupt:
        logger.info("Demo interrupted by user")
        sys.exit(1)
    except Exception as e:
        logger.error(f"Unhandled exception: {e}", exc_info=True)
        sys.exit(1)
    finally:
        server_manager.shutdown_servers()


if __name__ == "__main__":
    main()
