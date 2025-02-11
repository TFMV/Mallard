"""
LetSQL Multi-Engine Query Demo with Benchmarks

This demo showcases federated querying across Parquet, Postgres, and DuckDB,
with caching and result persistence. Includes performance measurements.

Usage:
    streamlit run this_file.py
"""

import time
import streamlit as st
import letsql as ls
from letsql.common.caching import ParquetCacheStorage
from letsql.expr.relations import into_backend
import pandas as pd
import numpy as np
import pyarrow as pa

# ------------------------------------------------------------------------------
# Benchmark Decorator
# ------------------------------------------------------------------------------


def benchmark(func):
    """Decorator to measure execution time of functions."""

    def wrapper(*args, **kwargs):
        start = time.perf_counter()
        result = func(*args, **kwargs)
        duration = time.perf_counter() - start
        st.write(f"⏱️ {func.__name__} took {duration:.2f} seconds")
        return result

    return wrapper


# ------------------------------------------------------------------------------
# Connection Setup Functions
# ------------------------------------------------------------------------------


@benchmark
def setup_connections():
    """Create connections for all data sources."""
    con = ls.connect()  # Main connection for query execution

    # Postgres connection for sample data
    pg = ls.postgres.connect(
        host="localhost",
        port=5432,
        user="postgres",
        password="postgres",
        database="postgres",
    )

    # In-memory DuckDB for caching
    duckdb = ls.duckdb.connect()

    return con, pg, duckdb


# ------------------------------------------------------------------------------
# Data Loading and Query Building Functions
# ------------------------------------------------------------------------------


@benchmark
def generate_test_data(pg, duckdb):
    """Generate test data for the demo in both Postgres and DuckDB."""

    # Create a temporary connection for source data
    con = ls.connect()

    # Create weather data
    weather_data = pd.DataFrame(
        {
            "date": pd.date_range("2015-01-01", "2015-12-31", freq="D"),
            "temperature": np.random.uniform(40, 80, 365),
        }
    )
    weather_table = con.register(weather_data, "temp_weather")

    # Create airports data
    airports_data = pd.DataFrame(
        {
            "code": [
                "ATL",
                "DFW",
                "DEN",
                "ORD",
                "LAX",
                "CLT",
                "LAS",
                "PHX",
                "MCO",
                "SEA",
            ],
            "city": [
                "Atlanta",
                "Dallas/Fort Worth",
                "Denver",
                "Chicago",
                "Los Angeles",
                "Charlotte",
                "Las Vegas",
                "Phoenix",
                "Orlando",
                "Seattle",
            ],
            "latitude": [
                33.6367,
                32.8998,
                39.8561,
                41.9742,
                33.9416,
                35.2144,
                36.0840,
                33.4342,
                28.4294,
                47.4502,
            ],
            "longitude": [
                -84.4281,
                -97.0403,
                -104.6737,
                -87.9073,
                -118.4085,
                -80.9473,
                -115.1537,
                -112.0080,
                -81.3089,
                -122.3088,
            ],
        }
    )
    airports_table = con.register(airports_data, "temp_airports")

    # Use into_backend to transfer to Postgres
    weather_pg = into_backend(weather_table, pg, "weather")
    airports_pg = into_backend(airports_table, pg, "airports")

    # Execute to materialize the tables
    ls.execute(weather_pg)
    ls.execute(airports_pg)

    st.write("✅ Created test data in Postgres")


@benchmark
def load_source_data(con, pg, duckdb, parquet_path):
    """Load data from different sources using LetSQL."""
    try:
        # Load Parquet data
        st.write("📊 Loading Parquet file...")
        flights_data = ls.read_parquet(parquet_path)

        # Show sample of Parquet data
        sample = ls.execute(flights_data.limit(5))
        st.write("✅ Loaded Parquet data")
        st.write("Sample data:", sample)

        # Transfer flights data to Postgres using into_backend
        st.write("📝 Creating flights table in Postgres...")
        flights_pg = into_backend(flights_data, pg, "flights")
        ls.execute(flights_pg)
        st.write("✅ Transferred flights data to Postgres")

        # Generate and load test data
        st.write("📊 Generating test data...")
        generate_test_data(pg, duckdb)

        # Get table references from Postgres
        flights = pg.table("flights")
        weather = pg.table("weather")
        airports = pg.table("airports")

        return flights, weather, airports

    except Exception as e:
        st.error("❌ Error loading data")
        st.exception(e)
        raise


def build_federated_query(flights, weather, airports, cache):
    """Build a query that joins data from all three sources."""
    st.write("Flights Schema:", flights.columns)
    
    # First check if we have data in each table
    st.write("Checking data in source tables...")
    flight_count = ls.execute(flights.count())  # Returns scalar
    weather_count = ls.execute(weather.count())  # Returns scalar
    st.write(f"Flights: {flight_count:,} rows")
    st.write(f"Weather: {weather_count:,} rows")
    
    # Build query with less restrictive filter
    query = (
        flights.join(
            weather,
            flights.FL_DATE == weather.date
        )
        .filter(flights.AIR_TIME > 0)  # Less restrictive filter
        .group_by([
            flights.FL_DATE,
            weather.temperature
        ])
        .aggregate(
            avg_delay=flights.DEP_DELAY.mean(),
            flight_count=flights.FL_DATE.count(),
            avg_air_time=flights.AIR_TIME.mean(),
            avg_distance=flights.DISTANCE.mean()
        )
        .cache(storage=cache)
    )
    
    # Preview the query results
    preview = ls.execute(query.limit(5))
    st.write("Query Preview:", preview)
    
    return query


# ------------------------------------------------------------------------------
# Persisting Results Functions
# ------------------------------------------------------------------------------


@benchmark
def persist_results_to_postgres(result_df, pg, table_name: str = "federated_results"):
    """
    Persist the query results into a Postgres table using LetSQL.

    Args:
        result_df: The query results DataFrame.
        pg: The Postgres connection.
        table_name: The name of the target Postgres table.
    """
    try:
        st.write(f"📝 Creating {table_name} table in Postgres...")

        # Check if we have any results
        if result_df.empty:
            st.warning("⚠️ No results to persist - query returned 0 rows")
            return

        # Create a temporary connection to hold the source data
        con = ls.connect()
        
        # Convert DataFrame to Arrow table first if needed
        if isinstance(result_df, pd.DataFrame):
            arrow_table = pa.Table.from_pandas(result_df)
        else:
            arrow_table = result_df
        
        # Register the table with the connection
        source_table = con.register(arrow_table, "temp_source")

        # Use into_backend to transfer to Postgres
        postgres_table = into_backend(source_table, pg, table_name)
        ls.execute(postgres_table)

        st.write(f"✅ Persisted results to Postgres table '{table_name}'")

    except Exception as e:
        st.error(f"❌ Error persisting to Postgres: {str(e)}")
        st.exception(e)
        raise


@benchmark
def cache_results_in_duckdb(result_df, duckdb_conn, table_name: str = "query_cache"):
    """Cache results in DuckDB using native DuckDB methods."""
    try:
        # Check if we have any results
        if result_df.empty:
            st.warning("⚠️ No results to cache - query returned 0 rows")
            return result_df

        # Ensure result_df is a Pandas DataFrame
        if not isinstance(result_df, pd.DataFrame):
            result_df = result_df.to_pandas()

        st.write(f"📝 Creating {table_name} table in DuckDB...")

        # Drop existing table if it exists
        duckdb_conn.con.execute(f"DROP TABLE IF EXISTS {table_name}")

        # Register the DataFrame as a temporary view
        duckdb_conn.con.register("temp_view", result_df)

        # Create a persistent table from the temporary view
        duckdb_conn.con.execute(f"""
            CREATE TABLE {table_name} AS 
            SELECT * FROM temp_view
        """)

        # Unregister the temporary view
        duckdb_conn.con.unregister("temp_view")

        # Verify and return the cached table as a DataFrame
        result = duckdb_conn.con.execute(f"SELECT * FROM {table_name}").fetchdf()
        st.write(f"✅ Cached {len(result):,} rows in DuckDB")
        return result

    except Exception as e:
        st.error(f"❌ Error caching in DuckDB: {str(e)}")
        st.exception(e)
        raise


# ------------------------------------------------------------------------------
# Main Function and Streamlit UI
# ------------------------------------------------------------------------------


def main():
    st.title("🚀 LetSQL Federated Query Demo")

    # Move demo info to sidebar
    with st.sidebar:
        st.header("About this Demo")
        st.write("""
        This demo showcases federated querying across multiple data sources:
        
        1️⃣ **Data Sources**
        - Flight data from Parquet
        - Weather data in Postgres
        - Airport data in Postgres
        
        2️⃣ **Operations**
        - Loading and transforming data
        - Executing federated queries
        - Caching results in DuckDB
        - Persisting results in Postgres
        
        3️⃣ **Features**
        - Cross-engine querying
        - Performance benchmarking
        - Data validation
        - Error handling
        """)
        
        st.divider()
        st.caption("Each step is benchmarked for performance analysis.")

    # Setup Phase
    st.write("## 1️⃣ Setting up Connections")
    con, pg, duckdb = setup_connections()
    cache = ParquetCacheStorage(source=con)

    # Data Loading Phase
    st.write("## 2️⃣ Loading Source Data")
    flights, weather, airports = load_source_data(
        con,
        pg,
        duckdb,
        "/Users/thomasmcgeehan/letsql-demo/letsql-demo/data/flights.parquet",
    )

    # Query Building Phase
    st.write("## 3️⃣ Building Federated Query")
    query = build_federated_query(flights, weather, airports, cache)
    st.code(str(query))

    # Query Execution Phase
    st.write("## 4️⃣ Executing Query")
    st.write(f"Query cached? {query.ls.exists()}")
    result = ls.execute(query)
    
    if result.empty:
        st.warning("⚠️ Query returned no results. Check your join conditions and filters.")
        st.stop()
        
    st.write(f"Query cached after execution? {query.ls.exists()}")

    # Results Display
    st.write("## 5️⃣ Query Results")
    with st.expander("View Results"):
        st.dataframe(result)

    # Continue with caching and persistence only if we have results
    if not result.empty:
        # Caching Phase
        st.write("## 6️⃣ Caching Results in DuckDB")
        cached_results = cache_results_in_duckdb(result, duckdb)
        st.dataframe(cached_results)

        # Persistence Phase
        st.write("## 7️⃣ Persisting Results to Postgres")
        persist_results_to_postgres(result, pg)
        st.success("✅ Demo completed successfully!")
    else:
        st.warning("⚠️ Demo completed with no results to cache or persist")


if __name__ == "__main__":
    main()
