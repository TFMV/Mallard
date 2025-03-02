import base64
import logging
import secrets
import os
import signal
import sys
import threading
import time
import argparse
import warnings
from typing import Dict, List, Optional, Any, Tuple

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from cloudpickle import loads

# Filter out Arrow alignment warnings
warnings.filterwarnings("ignore", message="An input buffer was poorly aligned")

# ------------------------------------------------------------------------------
# Logging Configuration
# ------------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mallard")


# ------------------------------------------------------------------------------
# Abstract Exchanger Interface
# ------------------------------------------------------------------------------
class AbstractExchanger:
    """Interface for custom data exchangers."""

    command = ""

    def exchange_f(self, context, reader, writer):
        raise NotImplementedError("Subclasses must implement exchange_f")


# ------------------------------------------------------------------------------
# Default Exchanger Implementation
# ------------------------------------------------------------------------------
class MyStreamingExchanger(AbstractExchanger):
    """
    A simple streaming exchanger that adds a 'processed' column.
    This class has one responsibility: transforming incoming Arrow data.
    """

    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        start_time = time.time()
        total_rows = 0
        batch_count = 0

        logger.info("MyStreamingExchanger processing data")

        all_incoming = []
        try:
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
        except Exception as e:
            logger.error(f"Error reading data: {e}")
            writer.begin(pa.schema([]))
            writer.close()
            return

        if not all_incoming:
            logger.info("No data received in exchanger")
            writer.begin(pa.schema([]))
            writer.close()
            return

        try:
            table_in = pa.Table.from_batches(all_incoming)
            processing_time = time.time() - start_time
            logger.info(f"Processed {total_rows:,} rows in {batch_count} batches")
            logger.info(f"Processing time: {processing_time*1000:.2f} ms")

            processed_col = pa.array([True] * table_in.num_rows, pa.bool_())
            table_out = table_in.append_column("processed", processed_col)

            writer.begin(table_out.schema)
            send_start = time.time()
            for batch in table_out.to_batches():
                writer.write_batch(batch)
            writer.close()
            send_time = time.time() - send_start
            logger.info(f"Response sent in {send_time*1000:.2f} ms")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise


# ------------------------------------------------------------------------------
# Authentication Middleware
# ------------------------------------------------------------------------------
class AuthMiddlewareFactory(flight.ServerMiddlewareFactory):
    """
    Middleware factory that handles authentication.
    This class is responsible only for validating credentials.
    """

    def __init__(self, credentials: Dict[str, str]):
        self.credentials = credentials
        self.tokens = {}

    def start_call(self, info, headers):
        auth_header = next(
            (headers[k][0] for k in headers if k.lower() == "authorization"), None
        )
        if not auth_header:
            raise flight.FlightUnauthenticatedError("No credentials supplied")

        auth_type, _, value = auth_header.partition(" ")
        if auth_type == "Basic":
            return self._handle_basic_auth(value)
        elif auth_type == "Bearer":
            return self._handle_token_auth(value)
        raise flight.FlightUnauthenticatedError("Invalid authentication type")

    def _handle_basic_auth(self, value):
        try:
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")
            if password != self.credentials.get(username):
                raise flight.FlightUnauthenticatedError("Invalid username or password")
            token = secrets.token_urlsafe(32)
            self.tokens[token] = username
            return AuthMiddleware(token)
        except Exception as e:
            logger.error(f"Basic auth error: {e}")
            raise flight.FlightUnauthenticatedError("Authentication failed")

    def _handle_token_auth(self, token):
        username = self.tokens.get(token)
        if username is None:
            raise flight.FlightUnauthenticatedError("Invalid token")
        return AuthMiddleware(token)


class AuthMiddleware(flight.ServerMiddleware):
    """Implements token-based authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        return {"authorization": f"Bearer {self.token}"}


# ------------------------------------------------------------------------------
# Database Manager
# ------------------------------------------------------------------------------
class DatabaseManager:
    """
    Encapsulates the DuckDB connection.
    This class is responsible solely for managing the database connection.
    """

    def __init__(self, db_path: str):
        self.db_path = db_path
        self.connection = self._create_connection()

    def _create_connection(self):
        if self.db_path != ":memory:":
            os.makedirs(os.path.dirname(os.path.abspath(self.db_path)), exist_ok=True)
        return duckdb.connect(self.db_path)

    def close(self):
        self.connection.close()


# ------------------------------------------------------------------------------
# Flight Server Configuration
# ------------------------------------------------------------------------------
class FlightServerConfig:
    """
    Holds configuration for a Flight server instance.
    Designed to be extended (Open/Closed Principle) without modification.
    """

    def __init__(
        self,
        location: str,
        db_path: str = ":memory:",
        server_id: Optional[str] = None,
        auth_enabled: bool = False,
        credentials: Optional[Dict[str, str]] = None,
    ):
        self.location = location
        self.db_path = db_path
        self.server_id = server_id or secrets.token_hex(4)
        self.auth_enabled = auth_enabled
        self.credentials = credentials or {"admin": "password123"}

    @classmethod
    def from_dict(cls, config_dict: Dict[str, Any]) -> "FlightServerConfig":
        return cls(
            location=config_dict["location"],
            db_path=config_dict.get("db_path", ":memory:"),
            server_id=config_dict.get("server_id"),
            auth_enabled=config_dict.get("auth_enabled", False),
            credentials=config_dict.get("credentials"),
        )

    def __str__(self) -> str:
        return (
            f"FlightServerConfig(location={self.location}, server_id={self.server_id})"
        )


# ------------------------------------------------------------------------------
# DuckDB Flight Server
# ------------------------------------------------------------------------------
class DuckDBFlightServer(flight.FlightServerBase):
    """
    Implements a Flight server using DuckDB.
    Dependencies (configuration, database manager, exchangers) are injected.
    """

    def __init__(
        self,
        config: FlightServerConfig,
        shutdown_event: threading.Event,
        database_manager: Optional[DatabaseManager] = None,
    ):
        super().__init__(config.location)
        self.config = config
        self.shutdown_event = shutdown_event
        self.server_id = config.server_id
        self.database_manager = database_manager or DatabaseManager(config.db_path)
        self.db_conn = self.database_manager.connection

        if config.auth_enabled:
            self.middleware = {"auth": AuthMiddlewareFactory(config.credentials)}

        self._shutdown_requested = False
        self.exchangers: Dict[str, AbstractExchanger] = {}
        self._register_default_exchangers()
        logger.info(f"Server {self.server_id} initialized at {config.location}")

    def _register_default_exchangers(self):
        # Pre-register the default exchanger
        streaming_exchanger = MyStreamingExchanger()
        self.exchangers[streaming_exchanger.command] = streaming_exchanger
        logger.info(
            f"Default exchanger '{streaming_exchanger.command}' registered on {self.server_id}"
        )

    def health_check(self) -> bool:
        try:
            self.db_conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def serve(self) -> threading.Thread:
        logger.info(f"Starting server at {self.config.location}")
        server_thread = threading.Thread(target=self._serve_thread, daemon=True)
        server_thread.start()
        return server_thread

    def _serve_thread(self):
        try:
            self.wait()
        except Exception as e:
            if not self._shutdown_requested:
                logger.error(f"Error in server thread: {e}")
            if self.shutdown_event:
                self.shutdown_event.set()

    def shutdown(self):
        if not self._shutdown_requested:
            logger.info(f"Shutting down server at {self.config.location}")
            self._shutdown_requested = True
            super().shutdown()
            try:
                self.database_manager.close()
                logger.info(f"Closed database connection for {self.config.location}")
            except Exception as e:
                logger.error(f"Error closing database: {e}")
            logger.info(f"Server at {self.config.location} shut down")

    # --------------------------------------------------------------------------
    # Flight Protocol Implementations
    # --------------------------------------------------------------------------
    def do_exchange(self, context, descriptor, reader, writer):
        try:
            command = descriptor.command.decode("utf-8")
            logger.info(f"Server {self.server_id} received exchange request: {command}")
            if command in self.exchangers:
                exchanger = self.exchangers[command]
                exchanger.exchange_f(context, reader, writer)
                return
            if self._is_sql_query(command):
                self._handle_sql_exchange(command, writer)
                return
            available = list(self.exchangers.keys())
            error_msg = f"Unknown exchange command: {command}. Available: {available}"
            logger.error(error_msg)
            raise flight.FlightServerError(error_msg)
        except Exception as e:
            logger.error(f"Error in do_exchange: {e}")
            raise

    def _is_sql_query(self, command: str) -> bool:
        sql_keywords = [
            "SELECT",
            "INSERT",
            "UPDATE",
            "DELETE",
            "CREATE",
            "DROP",
            "ALTER",
            "WITH",
        ]
        return any(command.upper().startswith(kw) for kw in sql_keywords)

    def _handle_sql_exchange(self, command: str, writer: flight.FlightDataStream):
        logger.info(f"Executing SQL query via exchange: {command}")
        result_table = self.db_conn.sql(command).fetch_arrow_table()
        writer.begin(result_table.schema)
        for batch in result_table.to_batches():
            writer.write_batch(batch)
        writer.close()
        logger.info("SQL query execution complete")

    def do_get(self, context, ticket):
        try:
            query = ticket.ticket.decode("utf-8")
            logger.info(f"Executing query: {query}")
            if self._is_ddl_statement(query):
                return self._handle_ddl_statement(query)
            result_table = self.db_conn.sql(query).fetch_arrow_table()
            return flight.RecordBatchStream(result_table)
        except Exception as e:
            logger.error(f"Error in do_get: {e}")
            raise

    def _is_ddl_statement(self, query: str) -> bool:
        return query.strip().upper().startswith(("CREATE", "DROP", "ALTER"))

    def _handle_ddl_statement(self, query: str) -> flight.FlightDataStream:
        self.db_conn.execute(query)
        return flight.RecordBatchStream(pa.table({"status": ["OK"]}))

    def do_put(self, context, descriptor, reader, writer):
        try:
            if hasattr(descriptor, "path") and descriptor.path:
                table_name = descriptor.path[0].decode("utf-8")
            else:
                table_name = descriptor.command.decode("utf-8")
            logger.info(f"Receiving data for table: {table_name}")

            batches = []
            while True:
                try:
                    batch, metadata = reader.read_chunk()
                    if batch.num_rows > 0:
                        batches.append(batch)
                except StopIteration:
                    break

            if not batches:
                logger.warning(f"No data received for {table_name}")
                return

            table = pa.Table.from_batches(batches)
            self._insert_table(table_name, table)
        except Exception as e:
            logger.error(f"Error in do_put: {e}")
            raise

    def _insert_table(self, table_name: str, table: pa.Table):
        temp_name = f"temp_{table_name}"
        self.db_conn.register(temp_name, table)
        self.db_conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM {temp_name} LIMIT 0;
            INSERT INTO {table_name} 
            SELECT * FROM {temp_name};
            """
        )
        self.db_conn.unregister(temp_name)
        logger.info(f"Inserted {table.num_rows:,} rows into {table_name}")

    def do_action(self, context, action):
        try:
            action_type = (
                action.type.decode("utf-8")
                if isinstance(action.type, bytes)
                else action.type
            )
            logger.info(f"Server {self.server_id} received action: {action_type}")
            if action_type == AddExchangeAction.name:
                return self._handle_add_exchange(action)
            logger.error(f"Unknown action: {action_type}")
            raise flight.FlightServerError(f"Unknown action: {action_type}")
        except Exception as e:
            logger.error(f"Error in do_action: {e}")
            raise

    def _handle_add_exchange(self, action) -> List[flight.Result]:
        exchanger_class = loads(action.body.to_pybytes())
        if not issubclass(exchanger_class, AbstractExchanger):
            raise ValueError("Exchanger must be a subclass of AbstractExchanger")
        exchanger_instance = exchanger_class()
        command = exchanger_instance.command
        self.exchangers[command] = exchanger_instance
        logger.info(f"Server {self.server_id} registered exchange: {command}")
        logger.info(f"Current exchangers: {list(self.exchangers.keys())}")
        return [flight.Result(f"Registered {command}".encode())]


# ------------------------------------------------------------------------------
# Flight Server Manager
# ------------------------------------------------------------------------------
class FlightServerManager:
    """
    Manages the lifecycle of one or more Flight server instances.
    This class is responsible solely for starting and shutting down servers.
    """

    def __init__(self, configs: Optional[List[FlightServerConfig]] = None):
        # By default, only one server is started; the demo may supply more.
        self.configs = configs or [
            FlightServerConfig(location="grpc://localhost:8815", server_id="server1")
        ]
        self.shutdown_event = threading.Event()
        self.running_servers: List[Tuple[DuckDBFlightServer, threading.Thread]] = []

    def start_servers(self):
        logger.info("Starting Flight servers...")
        for config in self.configs:
            self._start_server(config)
        self._register_signal_handlers()
        logger.info(f"Started {len(self.running_servers)} server(s)")

    def _start_server(self, config: FlightServerConfig):
        server = DuckDBFlightServer(config, self.shutdown_event)
        server_thread = server.serve()
        self.running_servers.append((server, server_thread))
        logger.info(f"Server {config.server_id} started at {config.location}")

    def shutdown_servers(self):
        logger.info("Shutting down all Flight servers...")
        self.shutdown_event.set()
        for server, thread in self.running_servers:
            try:
                server.shutdown()
                thread.join(timeout=5)
                if thread.is_alive():
                    logger.warning(
                        f"Server thread for {server.server_id} did not exit in time"
                    )
            except Exception as e:
                logger.error(f"Error shutting down server {server.server_id}: {e}")
        self.running_servers.clear()
        logger.info("All Flight servers shut down")

    def _register_signal_handlers(self):
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)
        try:
            signal.signal(signal.SIGABRT, self._handle_shutdown_signal)
        except AttributeError:
            pass

    def _handle_shutdown_signal(self, signum, frame):
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown_servers()
        sys.exit(0)


# ------------------------------------------------------------------------------
# Action for Adding an Exchanger
# ------------------------------------------------------------------------------
class AddExchangeAction:
    name = "add_exchange"


# ------------------------------------------------------------------------------
# Main Entry Point (optional)
# ------------------------------------------------------------------------------
def main():
    parser = argparse.ArgumentParser(description="DuckDB Flight Server")
    parser.add_argument(
        "--location",
        type=str,
        default="grpc://localhost:8815",
        help="Server location",
    )
    parser.add_argument(
        "--db",
        type=str,
        default=":memory:",
        help="Database path (':memory:' for in-memory)",
    )
    parser.add_argument("--auth", action="store_true", help="Enable authentication")
    args = parser.parse_args()

    config = FlightServerConfig(
        location=args.location,
        db_path=args.db,
        server_id="server1",
        auth_enabled=args.auth,
    )
    manager = FlightServerManager([config])
    try:
        manager.start_servers()
        while not manager.shutdown_event.is_set():
            time.sleep(1)
    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    finally:
        manager.shutdown_servers()


if __name__ == "__main__":
    main()
