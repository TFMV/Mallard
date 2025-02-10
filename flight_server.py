import base64
import logging
import secrets
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from cloudpickle import loads
from letsql.flight.action import AddExchangeAction
from letsql.flight.exchanger import AbstractExchanger
from threading import Thread, Event
import signal
import sys
import threading
import os

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DuckWings")

# Default configurations
SERVER_CONFIGS = [
    {"db_path": "duckwings1.db", "location": "grpc://localhost:8815"},
    {"db_path": "duckwings2.db", "location": "grpc://localhost:8816"},
]

class MyStreamingExchanger(AbstractExchanger):
    command = "my_streaming_exchanger"

    def exchange_f(self, context, reader, writer):
        # Process incoming data
        incoming_batches = []
        for chunk in reader:
            incoming_batches.append(chunk)

        # Echo the data back
        if incoming_batches:
            writer.begin(incoming_batches[0].schema)
            for batch in incoming_batches:
                writer.write_batch(batch)
        writer.close()


# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("DuckWings")

# Default configurations
SERVER_CONFIGS = [
    {"db_path": "duckwings1.db", "location": "grpc://localhost:8815"},
    {"db_path": "duckwings2.db", "location": "grpc://localhost:8816"},
]

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
        try:
            self.serve()
        except Exception as e:
            logger.error(f"Error in server {self.location}: {e}")
        finally:
            logger.info(f"Shutting down server at {self.location}")
            self.shutdown()

    def do_exchange(self, context, descriptor, reader, writer):
        """Handles streaming queries between DuckDB instances using Arrow Flight."""
        command = descriptor.command.decode("utf-8")

        if command in self.exchangers:
            logger.info(f"Executing exchanger: {command}")
            return self.exchangers[command].exchange_f(context, reader, writer)

        else:
            logger.info(f"Executing SQL query: {command}")
            try:
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
            self.conn.execute(f"""
                CREATE TABLE IF NOT EXISTS {table_name} AS 
                SELECT * FROM temp_{table_name} WHERE 1=0;
                
                INSERT INTO {table_name} 
                SELECT * FROM temp_{table_name};
            """)
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

            if action_type == AddExchangeAction.name:
                exchange_cls = loads(action.body.to_pybytes())
                if issubclass(exchange_cls, AbstractExchanger):
                    self.exchangers[exchange_cls.command] = (
                        exchange_cls()
                    )  # Create instance
                    logger.info(f"Registered exchange: {exchange_cls.command}")
                    return []
            raise flight.FlightServerError(f"Unknown action: {action_type}")
        except Exception as e:
            logger.exception(f"Error in do_action: {e}")
            raise


def start_server(server_config):
    """Starts a DuckDB Flight server."""
    server = DuckDBFlightServer(server_config["db_path"], server_config["location"])
    running_servers.append(server)  # Store instance for graceful shutdown
    try:
        server.serve_with_shutdown()
    except Exception as e:
        logger.error(f"Error in server {server.location}: {e}")


def handle_shutdown(signum, frame):
    """Handles Ctrl+C to gracefully shut down all servers."""
    logger.info("\nShutdown requested by user. Stopping all servers...")
    shutdown_event.set()

    # Properly shut down each running server
    for server in running_servers:
        logger.info(f"Shutting down server at {server.location}")
        try:
            server.shutdown()
        except Exception as e:
            logger.error(f"Error shutting down server at {server.location}: {e}")
        finally:
            logger.info(f"Server at {server.location} shut down successfully")


if __name__ == "__main__":
    # Register signal handler for Ctrl+C
    signal.signal(signal.SIGINT, handle_shutdown)
    signal.signal(signal.SIGTERM, handle_shutdown)
    signal.signal(signal.SIGABRT, handle_shutdown)

    try:
        # Start the servers
        threads = [
            Thread(target=start_server, args=(config,)) for config in SERVER_CONFIGS
        ]
        for thread in threads:
            thread.start()

        logger.info("Press Ctrl+C to stop the servers.")

        # Instead of a blocking join, join in a loop so that the main thread remains responsive.
        for thread in threads:
            while thread.is_alive():
                thread.join(timeout=1)

    except KeyboardInterrupt:
        handle_shutdown(None, None)
