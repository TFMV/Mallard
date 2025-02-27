import base64
import logging
import secrets
import os
import signal
import sys
import threading
import time
import argparse
from typing import Dict, List, Optional, Any, Tuple

import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from cloudpickle import loads

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s",
    handlers=[logging.StreamHandler()],
)
logger = logging.getLogger("mallard")


# ================================================================================
# Base Exchanger Interface and Implementation
# ================================================================================
class AbstractExchanger:
    """Base class for custom exchangers following the Interface Segregation Principle"""

    command = ""

    def exchange_f(self, context, reader, writer):
        """Method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement exchange_f")


class AddExchangeAction:
    """Action to add an exchanger to the server"""

    name = "add_exchange"


class MyStreamingExchanger(AbstractExchanger):
    """
    A simple streaming exchanger that adds a 'processed' column.
    Follows Single Responsibility Principle by focusing on one transformation.
    """

    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        """Process incoming data and add a 'processed' column."""
        start_time = time.time()
        total_rows = 0
        batch_count = 0

        logger.info("MyStreamingExchanger processing data")

        # Collect all incoming data
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
            # Return an empty table on error
            writer.begin(pa.schema([]))
            writer.close()
            return

        # Handle the case of no data
        if not all_incoming:
            logger.info("No data received in exchanger")
            writer.begin(pa.schema([]))
            writer.close()
            return

        # Process the data
        try:
            # Combine into one Arrow table
            table_in = pa.Table.from_batches(all_incoming)
            processing_time = time.time() - start_time

            # Log processing metrics
            logger.info(f"Processed {total_rows:,} rows in {batch_count} batches")
            logger.info(f"Processing time: {processing_time:.2f} seconds")

            # Add the processed column
            processed_col = pa.array([True] * table_in.num_rows, pa.bool_())
            table_out = table_in.append_column("processed", processed_col)

            # Stream the result back
            writer.begin(table_out.schema)
            send_start = time.time()

            for batch in table_out.to_batches():
                writer.write_batch(batch)

            writer.close()
            send_time = time.time() - send_start

            logger.info(f"Response sent in {send_time:.2f} seconds")
        except Exception as e:
            logger.error(f"Error processing data: {e}")
            raise


# ================================================================================
# Authentication Middleware
# ================================================================================
class AuthMiddlewareFactory(flight.ServerMiddlewareFactory):
    """
    Middleware factory for authentication.
    Follows Single Responsibility Principle by handling only authentication.
    """

    def __init__(self, credentials: Dict[str, str]):
        self.credentials = credentials
        self.tokens = {}

    def start_call(self, info, headers):
        """Validate credentials at the start of every call."""
        auth_header = next(
            (headers[k][0] for k in headers if k.lower() == "authorization"), None
        )
        if not auth_header:
            raise flight.FlightUnauthenticatedError("No credentials supplied")

        auth_type, _, value = auth_header.partition(" ")

        if auth_type == "Basic":
            # Handle basic auth
            return self._handle_basic_auth(value)
        elif auth_type == "Bearer":
            # Handle token auth
            return self._handle_token_auth(value)

        raise flight.FlightUnauthenticatedError("Invalid authentication type")

    def _handle_basic_auth(self, value):
        """Handle Basic authentication."""
        try:
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")

            if password != self.credentials.get(username):
                raise flight.FlightUnauthenticatedError("Invalid username or password")

            # Generate a token for this session
            token = secrets.token_urlsafe(32)
            self.tokens[token] = username

            return AuthMiddleware(token)
        except Exception as e:
            logger.error(f"Basic auth error: {e}")
            raise flight.FlightUnauthenticatedError("Authentication failed")

    def _handle_token_auth(self, token):
        """Handle Bearer token authentication."""
        username = self.tokens.get(token)
        if username is None:
            raise flight.FlightUnauthenticatedError("Invalid token")

        return AuthMiddleware(token)


class AuthMiddleware(flight.ServerMiddleware):
    """Middleware that implements token-based authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


# ================================================================================
# Configuration Management
# ================================================================================
class ServerConfig:
    """
    Configuration for a server instance.
    Follows Open/Closed Principle by allowing extension without modification.
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
    def from_dict(cls, config_dict: Dict[str, Any]) -> "ServerConfig":
        """Create a ServerConfig from a dictionary."""
        return cls(
            location=config_dict["location"],
            db_path=config_dict.get("db_path", ":memory:"),
            server_id=config_dict.get("server_id"),
            auth_enabled=config_dict.get("auth_enabled", False),
            credentials=config_dict.get("credentials"),
        )

    def __str__(self) -> str:
        return f"ServerConfig(location={self.location}, server_id={self.server_id})"


# Default configurations
DEFAULT_CONFIGS = [
    ServerConfig(location="grpc://localhost:8815", server_id="server1"),
    ServerConfig(location="grpc://localhost:8816", server_id="server2"),
]


# ================================================================================
# Flight Server Implementation
# ================================================================================
class DuckDBFlightServer(flight.FlightServerBase):
    """
    DuckDB Flight server with proper dependency injection and error handling.
    """

    def __init__(self, config: ServerConfig, shutdown_event: threading.Event):
        """Initialize the server with configuration."""
        super().__init__(config.location)

        # Configuration
        self.config = config
        self.shutdown_event = shutdown_event
        self.server_id = config.server_id

        # Database connection
        self._initialize_database()

        # Middleware
        if config.auth_enabled:
            self.middleware = {"auth": AuthMiddlewareFactory(config.credentials)}

        # State
        self._running = True
        self._shutdown_requested = False

        # Exchangers - store in instance to allow per-server customization
        self.exchangers = {}

        logger.info(f"Server {self.server_id} initialized at {config.location}")

    def _initialize_database(self):
        """Initialize the database connection."""
        try:
            # Ensure directory exists for file-based DB
            if self.config.db_path != ":memory:":
                os.makedirs(
                    os.path.dirname(os.path.abspath(self.config.db_path)), exist_ok=True
                )

            # Connect to DuckDB
            self.db_conn = duckdb.connect(self.config.db_path)

            # Verify connection
            self.db_conn.execute("SELECT 1")
            logger.info(f"Connected to DuckDB at {self.config.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB: {e}")
            raise

    def health_check(self) -> bool:
        """Verify server and database are operational."""
        try:
            self.db_conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def serve(self) -> threading.Thread:
        """Start the server and handle graceful shutdown."""
        try:
            logger.info(f"Starting server at {self.config.location}")

            # Start the server in a separate thread
            server_thread = threading.Thread(target=self._serve_thread, daemon=True)
            server_thread.start()

            return server_thread
        except Exception as e:
            logger.error(f"Error starting server: {e}")
            raise

    def _serve_thread(self):
        """Thread function to run the server."""
        try:
            self.wait()
        except Exception as e:
            if not self._shutdown_requested:
                logger.error(f"Error in server thread: {e}")
            if self.shutdown_event:
                self.shutdown_event.set()

    def shutdown(self):
        """Properly shutdown the server and clean up resources."""
        if not self._shutdown_requested:
            logger.info(f"Shutting down server at {self.config.location}")

            # Mark as shutting down
            self._shutdown_requested = True

            # Shutdown Flight server
            super().shutdown()

            # Close database connection
            if hasattr(self, "db_conn"):
                try:
                    self.db_conn.close()
                    logger.info(
                        f"Closed database connection for {self.config.location}"
                    )
                except Exception as e:
                    logger.error(f"Error closing database: {e}")

            # Mark as stopped
            self._running = False

            logger.info(f"Server at {self.config.location} shut down")

    # ================================================================================
    # Flight Protocol Implementation
    # ================================================================================
    def do_exchange(self, context, descriptor, reader, writer):
        """
        Handle data exchange.
        Follows Open/Closed Principle by allowing new exchangers to be added.
        """
        try:
            command = descriptor.command.decode("utf-8")
            logger.info(f"Server {self.server_id} received exchange request: {command}")

            # Handle registered exchangers
            if command in self.exchangers:
                exchanger = self.exchangers[command]
                logger.info(f"Using exchanger: {type(exchanger).__name__}")
                exchanger.exchange_f(context, reader, writer)
                return

            # Handle SQL queries
            if self._is_sql_query(command):
                self._handle_sql_exchange(command, writer)
                return

            # Unknown command
            available = list(self.exchangers.keys())
            error_msg = f"Unknown exchange command: {command}. Available: {available}"
            logger.error(error_msg)
            raise flight.FlightServerError(error_msg)

        except Exception as e:
            logger.error(f"Error in do_exchange: {e}")
            raise

    def _is_sql_query(self, command: str) -> bool:
        """Check if a command looks like a SQL query."""
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
        """Execute a SQL query and stream results."""
        logger.info(f"Executing SQL query via exchange: {command}")

        # Execute the query
        result_table = self.db_conn.sql(command).fetch_arrow_table()

        # Stream the results
        writer.begin(result_table.schema)
        for batch in result_table.to_batches():
            writer.write_batch(batch)
        writer.close()

        logger.info("SQL query execution complete")

    def do_get(self, context, ticket):
        """Handle GET requests (execute SQL queries)."""
        try:
            query = ticket.ticket.decode("utf-8")
            logger.info(f"Executing query: {query}")

            # Handle DDL statements separately
            if self._is_ddl_statement(query):
                return self._handle_ddl_statement(query)

            # Execute regular query
            result_table = self.db_conn.sql(query).fetch_arrow_table()
            return flight.RecordBatchStream(result_table)

        except Exception as e:
            logger.error(f"Error in do_get: {e}")
            raise

    def _is_ddl_statement(self, query: str) -> bool:
        """Check if a query is a DDL statement."""
        return query.strip().upper().startswith(("CREATE", "DROP", "ALTER"))

    def _handle_ddl_statement(self, query: str) -> flight.FlightDataStream:
        """Execute a DDL statement and return a result."""
        self.db_conn.execute(query)
        return flight.RecordBatchStream(pa.table({"status": ["OK"]}))

    def do_put(self, context, descriptor, reader, writer):
        """Handle PUT requests (insert data)."""
        try:
            # Get the table name from the descriptor
            if hasattr(descriptor, "path") and descriptor.path:
                table_name = descriptor.path[0].decode("utf-8")
            else:
                table_name = descriptor.command.decode("utf-8")

            logger.info(f"Receiving data for table: {table_name}")

            # Read all batches
            batches = []
            while True:
                try:
                    batch, metadata = reader.read_chunk()
                    if batch.num_rows > 0:
                        batches.append(batch)
                except StopIteration:
                    break

            # Handle empty data case
            if not batches:
                logger.warning(f"No data received for {table_name}")
                return

            # Process the data
            table = pa.Table.from_batches(batches)
            self._insert_table(table_name, table)

        except Exception as e:
            logger.error(f"Error in do_put: {e}")
            raise

    def _insert_table(self, table_name: str, table: pa.Table):
        """Insert an Arrow table into DuckDB."""
        # Register the Arrow table
        temp_name = f"temp_{table_name}"
        self.db_conn.register(temp_name, table)

        # Create the table if it doesn't exist and insert data
        self.db_conn.execute(
            f"""
            CREATE TABLE IF NOT EXISTS {table_name} AS 
            SELECT * FROM {temp_name} LIMIT 0;
            
            INSERT INTO {table_name} 
            SELECT * FROM {temp_name};
            """
        )

        # Clean up
        self.db_conn.unregister(temp_name)

        logger.info(f"Inserted {table.num_rows:,} rows into {table_name}")

    def do_action(self, context, action):
        """
        Handle custom actions.
        Follows Open/Closed Principle by supporting extensible actions.
        """
        try:
            # Decode action type
            action_type = action.type
            if isinstance(action_type, bytes):
                action_type = action_type.decode("utf-8")

            logger.info(f"Server {self.server_id} received action: {action_type}")

            # Handle known actions
            if action_type == AddExchangeAction.name:
                return self._handle_add_exchange(action)

            # Unknown action
            logger.error(f"Unknown action: {action_type}")
            raise flight.FlightServerError(f"Unknown action: {action_type}")

        except Exception as e:
            logger.error(f"Error in do_action: {e}")
            raise

    def _handle_add_exchange(self, action) -> List[flight.Result]:
        """Handle the AddExchange action."""
        # Load the exchanger class
        exchanger_class = loads(action.body.to_pybytes())

        # Verify it's a valid exchanger
        if not issubclass(exchanger_class, AbstractExchanger):
            raise ValueError("Exchanger must be a subclass of AbstractExchanger")

        # Create an instance and store it
        exchanger_instance = exchanger_class()
        command = exchanger_instance.command
        self.exchangers[command] = exchanger_instance

        logger.info(f"Server {self.server_id} registered exchange: {command}")
        logger.info(f"Current exchangers: {list(self.exchangers.keys())}")

        # Return success result
        return [flight.Result(f"Registered {command}".encode())]


# ================================================================================
# Server Management
# ================================================================================
class ServerManager:
    """
    Manages multiple Flight servers.
    Follows Single Responsibility Principle by focusing on server lifecycle.
    """

    def __init__(self, configs: Optional[List[ServerConfig]] = None):
        """Initialize with server configurations."""
        self.configs = configs or DEFAULT_CONFIGS
        self.shutdown_event = threading.Event()
        self.running_servers = []  # List of (server, thread) tuples

    def start_servers(self):
        """Start all configured servers."""
        logger.info("Starting servers...")

        # Create and start each server
        for config in self.configs:
            try:
                self._start_server(config)
            except Exception as e:
                logger.error(f"Failed to start server {config.server_id}: {e}")
                self.shutdown_servers()
                raise

        # Register signal handlers for graceful shutdown
        self._register_signal_handlers()

        logger.info(f"Started {len(self.running_servers)} servers")

    def _start_server(self, config: ServerConfig):
        """Start a single server."""
        try:
            # Create the server
            server = DuckDBFlightServer(config, self.shutdown_event)

            # Start the server
            server_thread = server.serve()

            # Store the server and thread
            self.running_servers.append((server, server_thread))

            logger.info(f"Server {config.server_id} started at {config.location}")

            # Pre-register any exchangers
            self._register_default_exchangers(server)

            return server
        except Exception as e:
            logger.error(f"Error starting server at {config.location}: {e}")
            raise

    def _register_default_exchangers(self, server: DuckDBFlightServer):
        """Register default exchangers for a server."""
        # Add MyStreamingExchanger by default
        streaming_exchanger = MyStreamingExchanger()
        server.exchangers[streaming_exchanger.command] = streaming_exchanger
        logger.info(
            f"Pre-registered exchanger {streaming_exchanger.command} on {server.server_id}"
        )

    def shutdown_servers(self):
        """Shutdown all running servers."""
        logger.info("Shutting down all servers...")

        # Signal shutdown
        self.shutdown_event.set()

        # Wait for all servers to shutdown and properly clean up
        for server, thread in self.running_servers:
            try:
                # Shutdown the server
                server.shutdown()

                # Wait for the thread to finish
                thread.join(timeout=5)

                if thread.is_alive():
                    logger.warning(
                        f"Server thread for {server.server_id} did not exit in time"
                    )
            except Exception as e:
                logger.error(f"Error shutting down server {server.server_id}: {e}")

        # Clear the list
        self.running_servers.clear()

        logger.info("All servers shut down")

    def _register_signal_handlers(self):
        """Register signal handlers for graceful shutdown."""
        signal.signal(signal.SIGINT, self._handle_shutdown_signal)
        signal.signal(signal.SIGTERM, self._handle_shutdown_signal)

        # Also handle SIGABRT if available
        try:
            signal.signal(signal.SIGABRT, self._handle_shutdown_signal)
        except AttributeError:
            pass  # SIGABRT not available on all platforms

    def _handle_shutdown_signal(self, signum, frame):
        """Handle shutdown signals."""
        logger.info(f"Received signal {signum}, initiating shutdown...")
        self.shutdown_servers()
        sys.exit(0)


# ================================================================================
# Module Functions
# ================================================================================
def start_servers(configs: Optional[List[ServerConfig]] = None) -> ServerManager:
    """Start servers with the given configurations."""
    manager = ServerManager(configs)
    manager.start_servers()
    return manager


def shutdown_servers(manager: ServerManager):
    """Shutdown servers managed by the given manager."""
    manager.shutdown_servers()


# Global server manager for singleton access
_global_manager = None


def get_server_manager() -> ServerManager:
    """Get the global server manager, creating it if necessary."""
    global _global_manager
    if _global_manager is None:
        _global_manager = ServerManager()
    return _global_manager


# ================================================================================
# Main Entry Point
# ================================================================================
if __name__ == "__main__":
    # Parse command line arguments
    parser = argparse.ArgumentParser(description="DuckDB Flight Server")
    parser.add_argument(
        "--server1-location",
        type=str,
        default="grpc://localhost:8815",
        help="Location for server 1",
    )
    parser.add_argument(
        "--server2-location",
        type=str,
        default="grpc://localhost:8816",
        help="Location for server 2",
    )
    parser.add_argument(
        "--server1-db",
        type=str,
        default=":memory:",
        help="Database path for server 1 (':memory:' for in-memory)",
    )
    parser.add_argument(
        "--server2-db",
        type=str,
        default=":memory:",
        help="Database path for server 2 (':memory:' for in-memory)",
    )
    parser.add_argument("--auth", action="store_true", help="Enable authentication")
    args = parser.parse_args()

    # Create server configurations
    configs = [
        ServerConfig(
            location=args.server1_location,
            db_path=args.server1_db,
            server_id="server1",
            auth_enabled=args.auth,
        ),
        ServerConfig(
            location=args.server2_location,
            db_path=args.server2_db,
            server_id="server2",
            auth_enabled=args.auth,
        ),
    ]

    try:
        # Start the servers with custom configs
        manager = start_servers(configs)

        # Keep the main thread alive until shutdown is requested
        while not manager.shutdown_event.is_set():
            time.sleep(1)

    except KeyboardInterrupt:
        logger.info("Interrupted by user")
    except Exception as e:
        logger.error(f"Error: {e}")
    finally:
        # Ensure servers are shut down
        if "_global_manager" in globals() and _global_manager:
            _global_manager.shutdown_servers()
