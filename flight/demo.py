import time
import duckdb
import pyarrow as pa
import pyarrow.flight as flight
from pyarrow.flight import FlightDescriptor, Ticket
import sys
from cloudpickle import dumps


# Define our own AddExchangeAction to replace letsql dependency
class AddExchangeAction:
    """Action to add an exchanger to the server"""

    name = "add_exchange"


# Define our own AbstractExchanger to replace letsql dependency
class AbstractExchanger:
    """Base class for custom exchangers"""

    command = ""

    def exchange_f(self, context, reader, writer):
        """Method to be implemented by subclasses"""
        raise NotImplementedError("Subclasses must implement exchange_f")


SERVER1 = "grpc://localhost:8815"
SERVER2 = "grpc://localhost:8816"

# Connect to both servers
client1 = flight.connect(SERVER1)
client2 = flight.connect(SERVER2)


###############################################################################
# 1) Quick verification of the Flight connections
###############################################################################
def verify_flight_connection():
    query = "SELECT 42 AS answer"
    reader = client2.do_get(Ticket(query.encode("utf-8")))
    print("\nConnection Verified:\n", reader.read_all())


def wait_for_servers(timeout=30):
    """Wait for both servers to be ready."""
    start = time.time()
    servers = [(SERVER1, client1), (SERVER2, client2)]

    while time.time() - start < timeout:
        all_ready = True
        for location, client in servers:
            try:
                # Try a simple query to check if server is responsive
                reader = client.do_get(Ticket(b"SELECT 1"))
                reader.read_all()
            except Exception as e:
                all_ready = False
                print(f"Waiting for {location}... ({e.__class__.__name__})")
                break

        if all_ready:
            print("Both servers ready!")
            return True

        time.sleep(1)

    raise RuntimeError(f"Servers not ready after {timeout} seconds")


# Replace the simple connection code with handshake
try:
    wait_for_servers()
except Exception as e:
    print(f"Failed to connect to servers: {e}")
    sys.exit(1)


###############################################################################
# 2) Simple example: create & move a small table "foo" from Server1 → Server2
###############################################################################
def setup_table():
    query = """
    CREATE TABLE IF NOT EXISTS foo (id INTEGER, name VARCHAR);
    INSERT INTO foo VALUES (1, 'test'), (2, 'example');
    """
    client1.do_get(Ticket(query.encode("utf-8")))


setup_table()


def fetch_data_from_server1():
    reader = client1.do_get(Ticket(b"SELECT * FROM foo"))
    return reader.read_all()


data = fetch_data_from_server1()
print("\nData from Server 1:")
print(data.to_pandas())


def move_data_to_server2():
    descriptor = FlightDescriptor.for_command(b"foo")
    writer, _ = client2.do_put(descriptor, data.schema)

    for batch in data.to_batches():
        writer.write_batch(batch)

    writer.close()
    print("\nData moved to Server 2.")


move_data_to_server2()

reader = client2.do_get(Ticket(b"SELECT * FROM foo"))
print("\nData now in Server 2:")
print(reader.read_all().to_pandas())


###############################################################################
# 3) Load a local Parquet file into an Arrow Table using DuckDB
###############################################################################
def load_parquet_to_duckdb(filepath):
    conn = duckdb.connect()
    return conn.sql(f"SELECT * FROM '{filepath}'").fetch_arrow_table()


flights_table = load_parquet_to_duckdb("../data/flights.parquet")
print("\nLoaded flights data from Parquet")


###############################################################################
# 4) Send the flights data to Server1, confirm it's there
###############################################################################
def send_flights_to_server1():
    descriptor = FlightDescriptor.for_command(b"flights")
    writer, _ = client1.do_put(descriptor, flights_table.schema)

    for batch in flights_table.to_batches():
        writer.write_batch(batch)

    writer.close()
    print("\nFlights data sent to Server 1")


send_flights_to_server1()

reader = client1.do_get(Ticket(b"SELECT * FROM flights"))
print("\nFlights data on Server 1:")
print(reader.read_all().to_pandas())


###############################################################################
# 5) Benchmark a do_get/do_put style flight exchange (Server1 → Server2)
###############################################################################
def benchmark_flight_exchange():
    start_time = time.time()
    batch_count = 0

    # Pull data from Server1
    reader = client1.do_get(Ticket(b"SELECT * FROM flights"))

    # Push data to Server2
    descriptor_out = FlightDescriptor.for_command(b"flights")
    writer_out, _ = client2.do_put(descriptor_out, reader.schema)

    for chunk in reader:
        batch = chunk.data
        if batch.num_rows == 0:
            continue
        writer_out.write_batch(batch)
        batch_count += 1

        if batch_count % 100 == 0:
            print(f"Processed {batch_count} batches...")

    writer_out.close()

    print(f"\nSuccessfully streamed {batch_count} batches from Server 1 → Server 2.")
    print(f"Data exchange completed in {time.time() - start_time:.3f} sec")


benchmark_flight_exchange()


###############################################################################
# 6) DEMO of do_exchange with a custom "MyStreamingExchanger"
###############################################################################
class MyStreamingExchanger(AbstractExchanger):
    """
    A minimal example that shows how to do a custom streaming exchange.
    This will read batches from the client, add a 'processed' column,
    and send them back to the client.
    """

    command = "my_streaming_exchanger"

    @classmethod
    def exchange_f(cls, context, reader, writer):
        start_time = time.time()
        total_rows = 0
        batch_count = 0

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
            empty_table = pa.table({})
            writer.begin(empty_table.schema)
            writer.close()
            return

        # Combine into one Arrow table
        table_in = pa.Table.from_batches(all_incoming)
        processing_time = time.time() - start_time
        throughput = total_rows / processing_time if processing_time > 0 else 0

        print("\nMyStreamingExchanger received:")
        print(f"• {total_rows:,} rows")
        print(f"• {batch_count:,} batches")
        print(f"• {processing_time:.2f} seconds")
        print(f"• {throughput:,.0f} rows/second")

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

        print("\nMyStreamingExchanger sent response:")
        print(f"• {send_time:.2f} seconds")
        print(f"• {send_throughput:,.0f} rows/second")
