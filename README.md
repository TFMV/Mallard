# Mallard

## High-Performance Data Exchange with Arrow Flight and DuckDB

Mallard demonstrates high-performance data streaming between DuckDB instances using Apache Arrow Flight, showcasing efficient data transfer and custom exchange protocols.

## ✨ Features

- **Dual DuckDB Flight Servers**: Two independent servers each listening on a unique gRPC endpoint
- **Basic Authentication**: Username/password authentication with token-based session management
- **Custom Exchangers**: Advanced streaming with inline transformations
- **High Performance**: Optimized for throughput with batch processing
- **Flexible Data Operations**: Support for queries, data insertion, and bidirectional streaming

## 🔧 Known Issues and Troubleshooting

### Arrow Alignment Warnings

You may see warnings like this in the server terminal:

```
An input buffer was poorly aligned. This could lead to crashes or poor performance on some hardware.
```

These warnings come from Apache Arrow's Acero execution engine and indicate memory alignment issues. While they don't prevent the application from working, they may impact performance.

**Solutions:**

1. **Configure Arrow to ignore alignment issues** (recommended):

   Add this code at the beginning of your scripts:

   ```python
   import pyarrow as pa
   pa.set_option("compute.allow_unaligned_buffers", True)
   ```

2. **Ensure proper memory alignment** when creating Arrow arrays:

   ```python
   # Use pandas as an intermediary for better alignment
   import pandas as pd
   df = pd.DataFrame(your_data)
   table = pa.Table.from_pandas(df)
   ```

3. **Suppress the warnings** if they're just noise:

   ```python
   import warnings
   warnings.filterwarnings("ignore", message="An input buffer was poorly aligned")
   ```

## Benchmarks

| Metric        | Value        | Throughput             |
| ------------- | ------------ | ---------------------- |
| GET time      | 0.32 seconds | 74,077,045 rows/second |
| Transfer time | 0.44 seconds | 44,816,067 rows/second |
| Exchange time | 0.53 seconds | 45,692,884 rows/second |
| Total rows    | 24,000,000   |                        |

## 🏗 Architecture

```
   ┌───────────────┐         ┌───────────────┐
   │    Mallard    │         │    Mallard    │
   │  (DuckDB #1)  │  <----> │  (DuckDB #2)  │
   │   port 8815   │         │   port 8816   │
   └───────┬───────┘         └───────┬───────┘
           │                         │
           │  (gRPC/Arrow Flight)    │
           │                         │
      ┌────▼─-───────────────────────▼─────┐
      │        demo.py (Flight Client)     │
      │  - do_get, do_put, do_exchange     │
      │  - verifies, benchmarks, logs      │
      └────────────────────────────────────┘
```

## ⚙️ Requirements

- Python 3.8+
- Required packages:
  - pyarrow
  - duckdb
  - cloudpickle

```bash
pip install -r requirements.txt
```

## 🚀 Quick Start

1. Start the servers:

```bash
python flight/flight_server.py
```

2. Run the demo:

```bash
python flight/demo.py
```

## 🔧 Configuration Options

The server supports several command line options for customization:

```bash
# Run with default in-memory databases (same as no arguments)
python flight/flight_server.py --server1-db ":memory:" --server2-db ":memory:"

# Run with persistent database files
python flight/flight_server.py --server1-db "data/server1.db" --server2-db "data/server2.db"

# Change server locations
python flight/flight_server.py --server1-location "grpc://localhost:9000" --server2-location "grpc://localhost:9001"

# Enable authentication
python flight/flight_server.py --auth
```

All available options:

- `--server1-location`: URL for server 1 (default: grpc://localhost:8815)
- `--server2-location`: URL for server 2 (default: grpc://localhost:8816)
- `--server1-db`: Database path for server 1 (default: :memory:)
- `--server2-db`: Database path for server 2 (default: :memory:)
- `--auth`: Enable authentication

## 📖 Demo Walkthrough

The demo script performs these operations:

1. **Connection Verification**
   - Waits for servers to be ready
   - Runs a simple query to confirm connectivity

2. **Basic Table Operations**
   - Creates and populates table `foo` on Server1
   - Transfers it to Server2

3. **Large Dataset Handling**
   - Loads `flights.parquet` into memory
   - Transfers to Server1
   - Benchmarks transfer to Server2

4. **Custom Exchange Demo**
   - Uses `MyStreamingExchanger` for bidirectional streaming
   - Adds a `processed` column during exchange
   - Measures performance metrics

## 🔒 Security

Authentication is implemented via:

- Default credentials: `admin:password123`
- Token-based session management
- Basic auth middleware

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
