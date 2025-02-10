import pyarrow.flight as flight
import pyarrow as pa
import duckdb
import time

SERVER1 = "grpc://localhost:8815"
SERVER2 = "grpc://localhost:8816"

client1 = flight.connect(SERVER1)
client2 = flight.connect(SERVER2)


def verify_flight_connection():
    query = "SELECT 42 AS answer"
    reader = client2.do_get(flight.Ticket(query.encode("utf-8")))
    print("\nConnection Verified:\n", reader.read_all())


verify_flight_connection()


def setup_table():
    query = """
    CREATE TABLE IF NOT EXISTS foo (id INTEGER, name VARCHAR);
    INSERT INTO foo VALUES (1, 'test'), (2, 'example');
    """
    client1.do_get(flight.Ticket(query.encode("utf-8")))


setup_table()


def fetch_data_from_server1():
    reader = client1.do_get(flight.Ticket(b"SELECT * FROM foo"))
    return reader.read_all()


data = fetch_data_from_server1()
print("\nData from Server 1:")
print(data.to_pandas())


def move_data_to_server2():
    descriptor = flight.FlightDescriptor.for_command(b"foo")
    writer, _ = client2.do_put(descriptor, data.schema)

    for batch in data.to_batches():
        writer.write_batch(batch)

    writer.close()
    print("\nData moved to Server 2.")


move_data_to_server2()


reader = client2.do_get(flight.Ticket(b"SELECT * FROM foo"))
print("\nData now in Server 2:")
print(reader.read_all().to_pandas())


def load_parquet_to_duckdb(filepath):
    conn = duckdb.connect()
    return conn.sql(f"SELECT * FROM '{filepath}'").fetch_arrow_table()


flights_table = load_parquet_to_duckdb("data/flights.parquet")
print("\nLoaded flights data from Parquet")


def send_flights_to_server1():
    descriptor = flight.FlightDescriptor.for_command(b"flights")
    writer, _ = client1.do_put(descriptor, flights_table.schema)

    for batch in flights_table.to_batches():
        writer.write_batch(batch)

    writer.close()
    print("\nFlights data sent to Server 1")


send_flights_to_server1()

reader = client1.do_get(flight.Ticket(b"SELECT * FROM flights"))
print("\nFlights data on Server 1:")
print(reader.read_all().to_pandas())


def benchmark_flight_exchange():
    start_time = time.time()
    batch_count = 0

    reader = client1.do_get(flight.Ticket(b"SELECT * FROM flights"))

    descriptor_out = flight.FlightDescriptor.for_command(b"flights")
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

