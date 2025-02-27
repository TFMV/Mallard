# Mallard

## High-Performance Data Exchange with Arrow Flight and DuckDB

Mallard demonstrates high-performance data streaming between DuckDB instances using Apache Arrow Flight, showcasing efficient data transfer and custom exchange protocols.

## ✨ Features

- **Dual DuckDB Flight Servers**: Two independent servers each listening on a unique gRPC endpoint
- **Basic Authentication**: Username/password authentication with token-based session management
- **Custom Exchangers**: Advanced streaming with inline transformations
- **High Performance**: Optimized for throughput with batch processing
- **Flexible Data Operations**: Support for queries, data insertion, and bidirectional streaming

## Benchmarks

| Metric     | Value        | Throughput              |
| ---------- | ------------ | ----------------------- |
| GET time   | 0.07 seconds | 351,309,939 rows/second |
| PUT time   | 0.78 seconds | 30,584,318 rows/second  |
| Total time | 0.85 seconds | 28,134,837 rows/second  |

## 📂 Project Structure

```bash
├── data/                 # Data files
│   └── flights.parquet   # Example dataset for testing
├── flight/               # Core flight components
│   ├── flight_server.py  # DuckDB Flight servers with auth & custom protocols
│   └── demo.py           # Client demonstrating data exchange & benchmarking
└── README.md             # This documentation
```

### flight_server

- Launches two DuckDB-based Flight servers
- Implements basic authentication with username/password
- Supports `do_get`, `do_put`, and `do_exchange` operations
- Enables dynamic exchanger registration
- Provides graceful shutdown handling

### demo

- Connects to both servers
- Demonstrates table creation and transfer
- Loads and transfers Parquet data
- Implements custom streaming logic
- Benchmarks performance

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
