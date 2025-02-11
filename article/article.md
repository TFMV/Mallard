# Breaking Data Speed Limits: LetSQL Meets DuckDB and Arrow Flight

## TL;DR

- 240M+ rows/sec streaming throughput 🚀
- LetSQL abstracts Flight + Arrow + DataFusion for insane performance
- Rust-powered, but you don't need to write Rust
- Streaming-native ETL is the future (batch is fading)
- If you move large-scale data, this pattern changes everything.

## Introduction

> Python is easy, but it is not simple.
> Go is simple, but it is not easy.
> Rust is… well, Rust is complicated.

I have been coding for over 30 years.

I have met true masters of the craft, and I am not one of them.

But I've always cared about clean, performant code. Long before I even had the words for it. It was never just about the language. It was about how efficiently I could make something run.

That obsession with performance led me to Apache Arrow and Flight in Go—where raw speed comes at the cost of brutal complexity. Every optimization was a battle: tuning batch sizes, wrestling with memory management, squeezing microseconds from data transfers.

Arrow-go is powerful, but it demands respect. The moment you dive into Flight RPC and IPC streams, the gloves come off. You're no longer working with friendly abstractions—you’re grappling with the machinery itself. It’s not about writing clever code. It’s about making every decision count, because every decision has a cost.

And if you want to hit those mythical performance numbers?
You don’t just code.
You engineer.

The problem? I don't know a lick of Rust.

LetSQL does.

LetSQL features a Rust-powered engine that brings together Arrow, DataFusion, and Flight, making advanced data streaming approachable. It abstracts away low-level complexity while enabling streaming-native data exchange at breakneck speeds.

And it's fast.

Really fast.

## The Problem: Why Data Movement Is Still Hard

- Flight RPC is powerful, but complex
- Apache Arrow Flight is designed for high-performance data exchange, but using it efficiently still requires:
  - Optimizing batch sizes for throughput
  - Streaming efficiently to avoid bottlenecks
  - Handling schema evolution & serialization overhead
- ETL vs. Streaming: The Performance Bottleneck
  - Traditional ETL moves data in bulk, not streams → inefficient for real-time workloads.
A lot of modern architectures are still batch-first → streaming-native solutions aren't as widely adopted.
- Arrow-go vs. Rust-based solutions
  - Arrow-go is great, but Rust-based solutions (like LetSQL) are often better optimized for:
    - Memory safety (Rust's ownership model)
    - Multi-threading & concurrency (LetSQL taps into DataFusion's parallel query engine)
    - Better integration with DataFusion (native Rust ecosystem)

## The Solution: Experimentation That Paid Off

I built a LetSQL demo in 9 hours.

It achieved 240M rows/sec of streaming throughput.

This wasn't just some random experiment—I wanted to see what was possible with Arrow Flight and a Rust-powered data movement engine.

### 🏗 Architecture

```scss
   ┌───────────────┐         ┌───────────────┐
   │ letsql_server │         │  letsql_server│
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

### How It Works

Two DuckDB instances act as Flight servers.
Streaming data between them in real-time (leveraging Arrow Flight).
Custom Flight Exchanger (MyStreamingExchanger) → Allows inline transformations while streaming.

### What's Inside

- LetSQL Flight Server → Built-in Flight server implementation.
- MyStreamingExchanger → Custom Flight exchanger for inline transformations.
- Flight Client → Simple Flight client implementation for testing.

### What LetSQL Does Differently

- Built-in Flight Server → No need to manually wire up Arrow Flight RPC.
- Seamless Arrow & DataFusion integration → Automatic batch processing optimization.
- Supports batch & streaming queries → No need to choose between ETL and real-time.
Instead of pulling & processing data in bulk, we move it at the speed of memory.

## The Results (Hold On Tight) 🚀

⚡ 240M+ Rows/Sec Streaming Throughput

- Flight do_exchange() achieves full-streaming throughput (no blocking bulk transfers).
- Real-world benchmark:
  - Sent: 24M records in 0.10 sec (236M rows/sec)
  - Received: 24M records in 0.40 sec (59M rows/sec)

⚡ Compared to Traditional Methods

| Method | Performance | Why It's Slower |
|--------|-------------|-----------------|
| CSV over HTTP | < 1M rows/sec | Text parsing overhead |
| Batch Parquet Copy | ~10M rows/sec | Disk I/O bottleneck |
| Arrow Flight GET | 50-60M rows/sec | Pull-based, but no inline transforms |
| Flight Exchange (LetSQL) | 240M+ rows/sec 🚀 | True streaming, inline processing |

> We're no longer talking about incremental performance gains. This is a paradigm shift in how we move data.

## The Future: Where This Pattern Is Going

Instead of manually defining Flight servers, what if you could declare them like this?

```python
@flight_ops(do_get=True, do_exchange=True)
def my_expr():
    return duckdb.sql("SELECT * FROM flights")

expr.into_flight(name="flights")  # Automatically spins up Flight server
```

This keeps @flight_ops focused on defining Flight capabilities, while into_flight() actually spins up the server.

---

## 🚀 Chained Flight Servers (Distributed Query Engines)

Hussain Sultan (LetSQL) hinted at the future:

- Chaining multiple Flight servers into a processing pipeline, with each Flight instance handling different transformations.
- Think of it like distributed SQL, but for streaming queries → instead of pulling from a database, queries flow across multiple Flight-powered processing nodes.
- The LetSQL team is actively working on this, and it could redefine how we handle high-velocity data.

---

## 💡 What This Means for the Industry

- ETL as we know it will change → streaming-native data pipelines will outperform batch-based architectures.
- Flight will become the backbone of modern data movement → its efficiency scales better than traditional REST APIs or bulk load operations.
- The industry is heading towards declarative streaming workflows → LetSQL might be the first glimpse of that future.

## Final Thoughts: Why This Matters

This wasn't just an experiment—this is a preview of what's coming next in data engineering.

- LetSQL, Arrow Flight, and DuckDB are unlocking streaming-native data pipelines.
- 240M+ rows/sec isn't theoretical—it's achievable today.
- Flight-based architectures could replace traditional batch ETL in many cases.

> This was fun to build, and I'm excited to see where LetSQL and the broader data community take this next.
