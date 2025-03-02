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

```bash
An input buffer was poorly aligned. This could lead to crashes or poor performance on some hardware.
```

These warnings come from Apache Arrow's Acero execution engine and indicate memory alignment issues. While they don't prevent the application from working, they may impact performance.

**Note:** As of the latest version, these warnings are automatically suppressed in both the server and client code using Python's warnings module.

If you want to handle these warnings differently, you can:

1. **Configure Arrow to ignore alignment issues** (if your Arrow version supports it):

   ```python
   import pyarrow as pa
   pa.set_option("compute.allow_unaligned_buffers", True)  # For newer Arrow versions
   ```

2. **Ensure proper memory alignment** when creating Arrow arrays:

   ```python
   # Use pandas as an intermediary for better alignment
   import pandas as pd
   df = pd.DataFrame(your_data)
   table = pa.Table.from_pandas(df)
   ```

3. **Manually suppress the warnings** (already implemented in current version):

   ```python
   import warnings
   warnings.filterwarnings("ignore", message="An input buffer was poorly aligned")
   ```

## Benchmarks

| Metric        | Value      | Throughput              |
| ------------- | ---------- | ----------------------- |
| GET time      | 203.18 ms  | 118,119,463 rows/second |
| Transfer time | 367.50 ms  | 52,093,044 rows/second  |
| Exchange time | 472.36 ms  | 50,808,383 rows/second  |
| Total rows    | 24,000,000 |                         |

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
      │  - benchmarks, reports, metrics    │
      └────────────────────────────────────┘
```

The system consists of:

1. **Two DuckDB Flight Servers**:
   - Each server runs an in-memory DuckDB instance by default
   - Servers expose Arrow Flight interfaces for data operations
   - Support for custom exchange protocols via registered handlers

2. **Flight Client**:
   - Connects to both servers for data transfer operations
   - Implements benchmarking and performance measurement
   - Provides formatted reporting of results

3. **Data Flow**:
   - GET: Client retrieves data from a server
   - TRANSFER: Client moves data between servers
   - EXCHANGE: Client sends data to server, receives processed results

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

The demo will automatically use the full 24 million row dataset from `data/flights.parquet` if available.

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
   - Creates and populates a simple table on Server1
   - Transfers it to Server2

3. **Large Dataset Handling**
   - Loads the full 24 million row dataset from `flights.parquet`
   - Transfers to Server1
   - Benchmarks transfer to Server2

4. **Custom Exchange Demo**
   - Uses `CustomStreamingExchanger` for bidirectional streaming
   - Processes data during exchange
   - Measures performance metrics

5. **Benchmark Report**
   - Generates a formatted report with color-coded metrics
   - Shows throughput in rows per second for all operations
   - Compares performance across different operations

## 🔒 Security

Authentication is implemented via:

- Default credentials: `admin:password123`
- Token-based session management
- Basic auth middleware

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
