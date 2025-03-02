# 🦆 Mallard

High-performance data exchange between DuckDB instances using Apache Arrow Flight

[![Python 3.8+](https://img.shields.io/badge/python-3.8+-blue.svg)](https://www.python.org/downloads/)
[![Apache Arrow](https://img.shields.io/badge/Apache%20Arrow-Flight-orange.svg)](https://arrow.apache.org/)
[![DuckDB](https://img.shields.io/badge/DuckDB-powered-yellow.svg)](https://duckdb.org/)

## 📊 Benchmarks

| Operation | Time (ms) | Throughput (rows/sec) |
| --------- | --------- | --------------------- |
| GET       | 203.18    | 118,119,463           |
| TRANSFER  | 367.50    | 52,093,044            |
| EXCHANGE  | 472.36    | 50,808,383            |

*24 million rows processed*

## 🚀 Quick Start

```bash
# Clone and setup
git clone https://github.com/TFMV/Mallard.git
cd Mallard
uv venv
source .venv/bin/activate
uv pip install -r requirements.txt

# Run the demo (automatically starts servers)
python demo.py
```

## 🏗️ Architecture

```
┌─────────────┐         ┌─────────────┐
│  DuckDB #1  │◄─────►  │  DuckDB #2  │
│  port 8815  │         │  port 8816  │
└──────┬──────┘         └──────┬──────┘
       │                       │
       │    Arrow Flight       │
       │      (gRPC)           │
       │                       │
┌──────▼───────────────────────▼──────┐
│           Flight Client             │
│  • GET/PUT/EXCHANGE operations      │
│  • Benchmarking and metrics         │
└────────────────────────────────────┘
```

## ✨ Key Features

- **Dual DuckDB Flight Servers** with independent gRPC endpoints
- **In-memory or persistent** database options
- **Custom data exchangers** with inline transformations
- **High-performance** batch processing (100M+ rows/sec)
- **Authentication** with token-based sessions

## 🔧 Configuration

```bash
# Run with in-memory databases (default)
python flight_server.py

# Use persistent storage
python flight_server.py --server1-db "data/server1.db" --server2-db "data/server2.db"

# Custom server locations
python flight_server.py --server1-location "grpc://localhost:9000" --server2-location "grpc://localhost:9001"

# Enable authentication
python flight_server.py --auth
```

## 📖 Demo Walkthrough

1. **Connection** - Verifies server connectivity
2. **Basic Operations** - Creates and transfers a simple table
3. **Large Dataset** - Processes 24M rows from flights.parquet
4. **Custom Exchange** - Demonstrates bidirectional streaming
5. **Benchmark Report** - Generates performance metrics

## 🔍 Troubleshooting

### Arrow Alignment Warnings

```bash
An input buffer was poorly aligned. This could lead to crashes or poor performance on some hardware.
```

## 📝 License

MIT License - See [LICENSE](LICENSE) file for details.
