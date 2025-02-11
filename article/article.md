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

I have been coding for over 30 years. I started young (really young), so don't go picturing me as a relic just yet.

I have met true masters of the craft, and I am not one of them.

But I loved clean, performant code before I even knew what those words meant. It wasn't about the language;it was about how efficiently I could make something run.

That obsession with performance led me to Apache Arrow and Flight in Go, where raw speed comes at the cost of brutal complexity. Every optimization was a battle: tuning batch sizes, managing Arrow memory pools, minimizing allocations, and squeezing microseconds from data transfers.

Arrow-go is powerful, but it demands respect. The moment you dive into Flight RPC and IPC streams, the gloves come off. You're no longer working with friendly abstractions—you're grappling with the machinery itself. It's not about writing clever code. It's about making every decision count, because every decision has a cost.

And if you want to hit those mythical performance numbers?
You don't just code.
You engineer.

The problem? I don't know a lick of Rust.

LetSQL does.

LetSQL features a Rust-powered engine that brings together Arrow, DataFusion, and Flight, making advanced data streaming approachable. It abstracts away low-level complexity while enabling streaming-native data exchange at breakneck speeds.

And it's fast.

Really fast.

## The Pain That Started It All

Before I was deep in Arrow Flight and high-performance data streaming, I was stuck wrangling an enterprise ETL tool called Striim.

The job? Migrating thousands of databases from on-prem SQL Server to the cloud, all while keeping systems online. That meant Change Data Capture (CDC) for real-time updates and an initial full load of historical data.

The CDC part was fine—it trickled updates efficiently.

The initial load? A complete disaster.

Striim was brutally inefficient for bulk inserts. It fired off singleton inserts at scale, turning what should have been a fast data movement process into a bottleneck-laden grind.

We often had to fracture the "initial load" into half a dozen separate Striim applications just to keep things moving. Even then, it crawled, choking the databases and bringing everything to a standstill.

It wasn't just slow; it was the wrong model for the problem.

That frustration pushed me toward Apache Arrow, Flight, and streaming-native architectures—where moving large-scale data wasn't just fast, it was designed to be fast from the ground up.

## The Problem: Why Data Movement Is Still Hard

I've spent years chasing raw speed—pushing data faster, cutting inefficiencies, and wringing every last cycle out of a system. That obsession led me to Apache Arrow and Flight RPC in Go.

On paper, Arrow Flight is a dream: zero-copy columnar data, lightning-fast IPC, and an RPC layer built for streaming massive datasets. But theory and reality rarely align.

Using Flight efficiently isn't just about turning it on; it's about mastering the hidden complexities:

- Batching & Throughput – Get batch sizes wrong, and you'll throttle performance before the system even warms up.
- Streaming vs. Bottlenecks – One bad decision can turn streaming into a bottleneck, negating Flight's advantages.
- Schema Evolution & Serialization Overhead – Real-world data isn't neat, and simply dumping Arrow tables into Flight won't cut it.
I spent days profiling Flight RPC calls in Go, hunting microseconds, chasing optimizations that worked—mostly. But the real issue wasn't Go's Arrow implementation.

The difference was deeper.

- Memory Management – Rust's ownership model lets LetSQL squeeze out raw efficiency in ways Go can't.
- Concurrency & Multi-threading – LetSQL taps directly into DataFusion's parallel query engine, tackling workloads Go struggles with.
- Seamless DataFusion Integration – A native Rust ecosystem means less glue code, fewer workarounds, and more raw speed.

LetSQL didn't just make Arrow Flight faster; it made it practical.

### Why arrow-go Struggles Against Rust

- Memory Management: Rust's ownership model gives LetSQL an edge in raw efficiency.
- Concurrency & Multi-threading: LetSQL taps directly into DataFusion's parallel query engine, handling workloads Go struggles with.
- Seamless Integration with DataFusion: A native Rust ecosystem means less glue code, fewer workarounds, and more raw speed.

Rust isn't just faster. It eliminates entire classes of inefficiencies. Go's garbage collection can cause unpredictable latency, while LetSQL's Rust engine optimizes every step: from zero-copy memory handling to native DataFusion integration. The result? Raw speed with no compromises.

## The Solution: Experimentation That Paid Off

I built a LetSQL demo in 9 hours.

It achieved 240M rows/sec of streaming throughput.

This wasn't just some random experiment. I wanted to see what was possible with Arrow Flight and a Rust-powered data movement engine.

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

## The Results (Hold On to Your Butt) :t-rex:

⚡ 240M+ Rows/Sec Streaming Throughput

- Flight do_exchange() achieves full-streaming throughput (no blocking bulk transfers).
- Real-world benchmark:
  - Sent: 24M records in 0.10 sec (236M rows/sec)
  - Received: 24M records in 0.40 sec (59M rows/sec)

### ⚡ Compared to Traditional Methods

| Method | Performance | Why It's Slower |
|--------|-------------|-----------------|
| CSV over HTTP | < 1M rows/sec | Text parsing overhead |
| Batch Parquet Copy | ~10M rows/sec | Disk I/O bottleneck |
| Arrow Flight GET | 50-60M rows/sec | Pull-based, but no inline transforms |
| Flight Exchange (LetSQL) | 240M+ rows/sec 🚀 | True streaming, inline processing |

> We're no longer talking about incremental performance gains. This is a paradigm shift in how we move data.

## The Code

The code is available on [GitHub](https://github.com/TFMV/letsql-demo) but imagine if Flight servers, clients, and exchangers were this simple:

### 1️⃣ Spin up a Flight Server

```python
from letsql.flight.server import FlightServer

server = FlightServer(
    name="flights",
    do_get=True,
    do_exchange=True,
)

server.start()
```

No manual Arrow IPC wiring.
No complicated schema definitions.
Just declare what you need, and LetSQL handles the rest.

### 2️⃣ Connect a Flight Client

```python
from letsql.flight.client import FlightClient

client = FlightClient(
    name="flights",
    port=8815,
)
```

A single object that abstracts Flight tickets, descriptors, and connections—instead of forcing you to manage them manually.

### 3️⃣ Define a Custom Flight Exchanger

```python
    from letsql.flight.exchanger import MyStreamingExchanger

    exchanger = MyStreamingExchanger()
```

Instead of manually reading Arrow RecordBatches, transforming data, and writing results,
this would let you plug in a streaming transformer like a first-class citizen.

## Where LetSQL Stands Today

LetSQL is powerful, but setting up Flight Servers, Clients, and Exchangers still requires some boilerplate.

It doesn't have to.

Imagine a world where defining a Flight server is as simple as declaring a function. Where clients and exchangers are first-class citizens—plug-and-play, no friction, no clutter.

LetSQL already does the heavy lifting. A declarative, intuitive API would make that power truly effortless.

And the best part? The LetSQL team is already working on it.

## The Future: A Fully Declarative LetSQL

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

This wasn't just an experiment. This is a preview of what's coming next in data engineering.

- LetSQL, Arrow Flight, and DuckDB are unlocking streaming-native data pipelines.
- 240M+ rows/sec isn't theoretical—it's achievable today.
- Flight-based architectures could replace traditional batch ETL in many cases.

> This was fun to build, and I'm excited to see where LetSQL and the broader data community take this next.

Tom McGeehan is a high-performance data engineering specialist who has spent years exploring the bleeding edge of Arrow, Flight, and modern streaming architectures. His work focuses on making large-scale data movement faster, more efficient, and more intuitive. He writes about performance, systems engineering, and the future of real-time ETL.

---

## About the Author

I’m a performance-obsessed engineer who loves pushing data systems to their limits. With experience scaling enterprise data pipelines at 66degrees and a deep focus on high-performance architectures, I thrive on solving complex data engineering challenges.

- 🚀 Specializing in ultra-fast data movement and streaming architectures
- 💡 Deep expertise in Arrow Flight, DuckDB, and high-performance ETL
- 🛠 Built large-scale migration systems handling billions of records
- 📫 Open to challenging opportunities in streaming and performance-driven data systems

---

🔗 Connect on [LinkedIn](https://www.linkedin.com/in/tfmv/) | 🐙 [GitHub](https://github.com/TFMV)