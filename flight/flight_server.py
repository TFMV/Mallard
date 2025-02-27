import base64
import logging
import secrets
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from cloudpickle import loads
from threading import Thread, Event
import signal
import sys
import os
import time

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("Mallard")

# Default configurations
SERVER_CONFIGS = [
    {"db_path": ":memory:", "location": "grpc://localhost:8815"},
    {"db_path": ":memory:", "location": "grpc://localhost:8816"},
]


# Custom implementation to replace letsql.flight.exchanger.AbstractExchanger
class AbstractExchanger:
    """Base class for custom exchangers"""

    command = ""

    def exchange_f(self, context, reader, writer):
        """Method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement exchange_f")


# Custom implementation to replace letsql.flight.action.AddExchangeAction
class AddExchangeAction:
    """Action to add an exchanger to the server"""

    name = "add_exchange"


class MyStreamingExchanger(AbstractExchanger):
    """
    A minimal example that shows how to do a custom streaming exchange.
    This will read batches from the client, add a 'processed' column,
    and send them back to the client.
    """

    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        """Process incoming data and add a 'processed' column."""
        start_time = time.time()
        total_rows = 0
        batch_count = 0

        logger.info("MyStreamingExchanger starting to process data")

        all_incoming = []
        while True:
            try:
                chunk = reader.read_chunk()
            except StopIteration:
                break
            if chunk.data.num_rows == 0:
                break
            all_incoming.append(chunk.data)
            total_rows += chunk.data.num_rows
            batch_count += 1

        if not all_incoming:
            logger.info("No data received in exchanger")
            empty_table = pa.table({})
            writer.begin(empty_table.schema)
            writer.close()
            return

        # Combine into one Arrow table
        table_in = pa.Table.from_batches(all_incoming)
        processing_time = time.time() - start_time
        throughput = total_rows / processing_time if processing_time > 0 else 0

        logger.info("\nMyStreamingExchanger received:")
        logger.info(f"• {total_rows:,} rows")
        logger.info(f"• {batch_count:,} batches")
        logger.info(f"• {processing_time:.2f} seconds")
        logger.info(f"• {throughput:,.0f} rows/second")

        # Example: add a boolean column
        processed_col = pa.array([True] * table_in.num_rows, pa.bool_())
        table_out = table_in.append_column("processed", processed_col)

        # Stream back
        writer.begin(table_out.schema)
        send_start = time.time()
        for batch in table_out.to_batches():
            writer.write_batch(batch)

        writer.close()
        send_time = time.time() - send_start
        send_throughput = total_rows / send_time if send_time > 0 else 0

        logger.info("\nMyStreamingExchanger sent response:")
        logger.info(f"• {send_time:.2f} seconds")
        logger.info(f"• {send_throughput:,.0f} rows/second")


# Shutdown event to signal all servers to stop
shutdown_event = Event()

# Store running servers
running_servers = []


class BasicAuthServerMiddlewareFactory(flight.ServerMiddlewareFactory):
    """Middleware that implements username-password authentication."""

    def __init__(self, creds):
        self.creds = creds
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
            decoded = base64.b64decode(value).decode("utf-8")
            username, _, password = decoded.partition(":")
            if password != self.creds.get(username):
                raise flight.FlightUnauthenticatedError("Invalid username or password")

            token = secrets.token_urlsafe(32)
            self.tokens[token] = username
            return BasicAuthServerMiddleware(token)

        elif auth_type == "Bearer":
            username = self.tokens.get(value)
            if username is None:
                raise flight.FlightUnauthenticatedError("Invalid token")
            return BasicAuthServerMiddleware(value)

        raise flight.FlightUnauthenticatedError("No credentials supplied")


class BasicAuthServerMiddleware(flight.ServerMiddleware):
    """Middleware that implements username-password authentication."""

    def __init__(self, token):
        self.token = token

    def sending_headers(self):
        """Return the authentication token to the client."""
        return {"authorization": f"Bearer {self.token}"}


class DuckDBFlightServer(flight.FlightServerBase):
    """
    Flight server for DuckDB with support for GET, PUT, and Flight do_exchange
    for streaming queries.
    """

    def __init__(self, db_path: str, location: str):
        super().__init__(location)
        self.db_path = os.path.abspath(db_path)
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)

        # Initialize DuckDB
        self.conn = duckdb.connect(self.db_path, read_only=False)
        self.location = location
        self.middleware = {
            "auth": BasicAuthServerMiddlewareFactory({"admin": "password123"})
        }
        self._running = True
        self.exchangers = {}
        self._shutdown_requested = False

        # Add health check query to verify DB is ready
        try:
            self.conn.execute("SELECT 1")
            logger.info(f"✅ Connected to DuckDB at {self.db_path}")
        except Exception as e:
            logger.error(f"Failed to connect to DuckDB at {self.db_path}: {e}")
            raise

    def health_check(self) -> bool:
        """Verify server and database are operational."""
        try:
            self.conn.execute("SELECT 1")
            return True
        except Exception as e:
            logger.error(f"Health check failed: {e}")
            return False

    def serve_with_shutdown(self):
        """Runs the server until the shutdown event is set."""
        if not self.health_check():
            raise RuntimeError(f"Server at {self.location} failed health check")

        logger.info(f"Server started at {self.location}. Press Ctrl+C to stop.")

        # Start the server in a separate thread
        server_thread = Thread(target=self._serve_thread, daemon=True)
        server_thread.start()

        try:
            # Wait for shutdown event
            while not shutdown_event.is_set() and self._running:
                time.sleep(0.5)
        except Exception as e:
            if not shutdown_event.is_set():  # Only log if not due to shutdown
                logger.error(f"Error in server {self.location}: {e}")
        finally:
            logger.info(f"Shutting down server at {self.location}")
            self._shutdown_requested = True
            self.shutdown()
            # Close the database connection
            if hasattr(self, "conn") and self.conn:
                try:
                    self.conn.close()
                    logger.info(f"Closed database connection for {self.location}")
                except Exception as e:
                    logger.error(f"Error closing database connection: {e}")

    def _serve_thread(self):
        """Thread function to run the server."""
        try:
            self.serve()
        except Exception as e:
            if not self._shutdown_requested:  # Only log if not due to shutdown
                logger.error(f"Error in server thread {self.location}: {e}")
        finally:
            if not self._shutdown_requested:
                logger.info(f"Server thread at {self.location} exited unexpectedly")

    def do_exchange(self, context, descriptor, reader, writer):
        """Handles streaming queries between DuckDB instances using Arrow Flight."""
        command = descriptor.command.decode("utf-8")
        logger.info(f"Received exchange command: {command}")

        # First check if this is a registered custom exchanger
        if command in self.exchangers:
            logger.info(f"Executing custom exchanger: {command}")
            return self.exchangers[command].exchange_f(context, reader, writer)

        # If not a custom exchanger, treat it as a SQL query
        else:
            logger.info(f"Executing SQL query via exchange: {command}")
            try:
                # Validate that this looks like a SQL query to avoid confusion
                if not any(
                    keyword in command.upper()
                    for keyword in [
                        "SELECT",
                        "INSERT",
                        "UPDATE",
                        "DELETE",
                        "CREATE",
                        "DROP",
                        "ALTER",
                    ]
                ):
                    raise ValueError(
                        f"Command '{command}' is not a recognized SQL query or custom exchanger"
                    )

                result_table = self.conn.sql(command).fetch_arrow_table()

                writer.begin(result_table.schema)

                for batch in result_table.to_batches():
                    writer.write_batch(batch)

                writer.close()
                logger.info("Streaming query complete.")

            except Exception as e:
                logger.exception(f"Error in do_exchange: {e}")
                raise

    def do_get(self, context, ticket):
        """Handles GET requests to retrieve query results as Arrow Flight stream."""
        try:
            query = ticket.ticket.decode("utf-8")
            logger.info("Executing query: %s", query)

            # Handle DDL statements separately
            if query.strip().upper().startswith(("CREATE", "DROP", "ALTER")):
                self.conn.execute(query)
                # Return an empty table for DDL statements
                return flight.RecordBatchStream(pa.table({"status": ["OK"]}))
            else:
                result_table = self.conn.sql(query).fetch_arrow_table()
                return flight.RecordBatchStream(result_table)
        except Exception as e:
            logger.exception("Error in do_get: %s", e)
            raise

    def do_put(self, context, descriptor, reader, writer):
        """Handles PUT requests to insert data into DuckDB."""
        try:
            table = reader.read_all()
            table_name = descriptor.command.decode("utf-8")
            logger.info("Inserting data into table: %s", table_name)

            # Register the Arrow table and create/insert in one step
            self.conn.register(f"temp_{table_name}", table)
            self.conn.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM temp_{table_name} WHERE 1=0;
                
                INSERT INTO {table_name} 
                SELECT * FROM temp_{table_name};
            """
            )
            self.conn.unregister(f"temp_{table_name}")  # Clean up temporary table

            logger.info("Inserted %d rows into %s", table.num_rows, table_name)
        except Exception as e:
            logger.exception("Error in do_put: %s", e)
            raise

    def do_action(self, context, action):
        """Handles custom actions (e.g., registering an exchange)."""
        try:
            if isinstance(action.type, bytes):
                action_type = action.type.decode("utf-8")
            else:
                action_type = action.type

            logger.info(f"Received action: {action_type}")

            if action_type == AddExchangeAction.name:
                exchange_cls = loads(action.body.to_pybytes())
                if issubclass(exchange_cls, AbstractExchanger):
                    command = exchange_cls.command
                    self.exchangers[command] = exchange_cls()  # Create instance
                    logger.info(f"Registered exchange: {command}")
                    logger.info(f"Current exchangers: {list(self.exchangers.keys())}")
                    return []
                else:
                    logger.error(
                        f"Received invalid exchanger class: {exchange_cls.__name__}"
                    )
                    raise flight.FlightServerError(
                        f"Invalid exchanger class: {exchange_cls.__name__}"
                    )
            else:
                raise flight.FlightServerError(f"Unknown action: {action_type}")
        except Exception as e:
            logger.exception(f"Error in do_action: {e}")
            raise

    def shutdown(self):
        """Properly shutdown the server and clean up resources."""
        self._shutdown_requested = True
        super().shutdown()
        self._running = False
        logger.info(f"Server at {self.location} has been shut down")


def start_server(server_config):
    """Starts a DuckDB Flight server."""
    server = DuckDBFlightServer(server_config["db_path"], server_config["location"])
    running_servers.append(server)  # Store instance for graceful shutdown
    try:
        server.serve_with_shutdown()
    except Exception as e:
        if not shutdown_event.is_set():  # Only log if not due to shutdown
            logger.error(f"Error in server {server.location}: {e}")
    finally:
        # Ensure server is properly shut down even if an exception occurs
        if server._running:
            try:
                server.shutdown()
            except Exception as e:
                logger.error(f"Error during server shutdown: {e}")


def handle_shutdown(signum, frame):
    """Handles Ctrl+C to gracefully shut down all servers."""
    logger.info("\nShutdown requested. Stopping all servers...")
    shutdown_event.set()

    # Give servers a moment to notice the shutdown event
    time.sleep(1.0)

    # Properly shut down each running server
    for server in running_servers:
        if hasattr(server, "_running") and server._running:
            logger.info(f"Shutting down server at {server.location}")
            try:
                server._shutdown_requested = True
                server.shutdown()
            except Exception as e:
                logger.error(f"Error shutting down server at {server.location}: {e}")

    # Wait for all servers to finish shutting down (max 5 seconds)
    shutdown_start = time.time()
    while any(hasattr(s, "_running") and s._running for s in running_servers):
        if time.time() - shutdown_start > 5:
            logger.warning("Some servers did not shut down in time. Forcing exit.")
            break
        time.sleep(0.5)


if __name__ == "__main__":
    # Register signal handler for Ctrl+C and other termination signals
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)

    # Also handle SIGABRT if available on the platform
    try:
        signal.signal(signal.SIGABRT, handle_shutdown)
    except AttributeError:
        pass  # SIGABRT might not be available on all platforms

    try:
        # Start the servers
        threads = [
            Thread(target=start_server, args=(config,), daemon=True)
            for config in SERVER_CONFIGS
        ]
        for thread in threads:
            thread.start()

        logger.info("Press Ctrl+C to stop the servers.")

        # Wait for all threads to complete or until interrupted
        while any(thread.is_alive() for thread in threads):
            time.sleep(0.5)
            if shutdown_event.is_set():
                # Give threads time to clean up
                for thread in threads:
                    thread.join(timeout=2.0)
                break

    except KeyboardInterrupt:
        # This should be caught by the signal handler, but just in case
        handle_shutdown(None, None)

    finally:
        # Final cleanup
        if not shutdown_event.is_set():
            handle_shutdown(None, None)

        # Wait a moment for final cleanup
        time.sleep(0.5)

        # Check if any servers are still running and force shutdown if needed
        still_running = [
            s for s in running_servers if hasattr(s, "_running") and s._running
        ]
        if still_running:
            logger.warning(f"{len(still_running)} servers still running. Forcing exit.")
        else:
            logger.info("All servers have been shut down cleanly.")

        logger.info("Exiting.")
        sys.exit(0)
