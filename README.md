# letsql-demo

## High-Performance Data Exchange

This repository demonstrates how to use the letsql library to exchange data between DuckDB instances using Arrow Flight, showcasing high-performance streaming data transfer and custom exchange protocols.

## ✨ Features

- **Two DuckDB Flight Servers**: Each listening on a unique gRPC endpoint
- **Basic Authentication**: Username/password + token-based session management
- **Custom Exchangers**: Advanced streaming with inline transformations
- **High Performance**: 140M+ rows/second throughput in testing
- **Large Dataset Support**: Successfully tested with 200M+ row datasets

## 📂 Repository Structure

```bash
.
├── flight_server.py     # DuckDB Flight servers with auth & custom protocols
├── demo.py             # Client demonstrating data exchange & benchmarking
├── data/
│   └── flights.parquet # Example dataset for testing
└── README.md          # This documentation
```

### flight_server.py

- Launches two DuckDB-based Flight servers
- Implements basic authentication
- Supports `do_get`, `do_put`, and `do_exchange`
- Enables dynamic exchanger registration

### demo.py

- Connects to both servers
- Demonstrates table creation and transfer
- Loads and transfers Parquet data
- Implements custom streaming logic
- Benchmarks performance

## 🏗 Architecture

```scss
   ┌───────────────┐         ┌───────────────┐
   │ letsql_server │         │  letsql_server│
   │  (DuckDB #1)  │  <----> │  (DuckDB #2)  │
   │   port 8815   │         │   port 8816   │
   └───────┬───────┘         └───────┬───────┘
           │                         │
           │ (gRPC/Arrow Flight)     │
           │                         │
      ┌─────▼────────────────────────▼─────┐
      │        demo.py (Flight Client)     │
      │  - do_get, do_put, do_exchange     │
      │  - verifies, benchmarks, logs      │
      └────────────────────────────────────┘
```

## ⚙️ Requirements

- Python 3.8+
- Required packages:

```bash
pip install duckdb pyarrow pyarrow[flight] cloudpickle letsql
```

## 🚀 Quick Start

1. Start the servers:

```bash
python flight_server.py
```

2. Run the demo:

```bash
python demo.py
```

## 📖 Demo Walkthrough

The demo script performs these operations:

1. **Connection Verification**
   - Runs `SELECT 42` to confirm server connectivity

2. **Basic Table Operations**
   - Creates and populates table `foo` on Server1
   - Transfers it to Server2

3. **Large Dataset Handling**
   - Loads `flights.parquet` into memory
   - Transfers to Server1
   - Benchmarks transfer to Server2

4. **Custom Exchange Demo**
   - Registers `MyStreamingExchanger` on Server1
   - Demonstrates bidirectional streaming
   - Adds a `processed` column during exchange

## 📊 Performance

Recent benchmarks show:

- Send throughput: 140M+ rows/second
- Receive throughput: 50M+ rows/second
- Successfully tested with 200M+ row datasets

*Note: Actual performance depends on hardware, network, and dataset characteristics.*

## 🔒 Security

Authentication is implemented via:

- Default credentials: `admin:password123`
- Token-based session management
- Secure credential handling

## 🤝 Contributing

Contributions are welcome! Please feel free to submit a Pull Request.

## 📝 License

This project is licensed under the MIT License - see the [LICENSE](LICENSE) file for details.
