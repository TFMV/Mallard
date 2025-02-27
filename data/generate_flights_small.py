"""Generate a small flights dataset that will work with our federated query demo."""

import pandas as pd
import numpy as np

def generate_flights_data():
    """Generate flight data that matches our test weather and airports data."""
    
    # Create date range matching our weather data
    dates = pd.date_range('2015-01-01', '2015-12-31', freq='D')
    
    # Airport codes from our test data
    airport_codes = ['ATL', 'DFW', 'DEN', 'ORD', 'LAX', 'CLT', 'LAS', 'PHX', 'MCO', 'SEA']
    
    # Generate flights for each airport pair each day
    rows = []
    for date in dates:
        # Create multiple flights between different airport pairs
        for origin in airport_codes:
            # Each airport has flights to 3 random destinations per day
            destinations = np.random.choice(
                [code for code in airport_codes if code != origin],
                size=3,
                replace=False
            )
            for dest in destinations:
                # Create 2 flights for each route per day
                for _ in range(2):
                    # Generate realistic flight data
                    air_time = np.random.normal(120, 30)  # ~2 hours average flight
                    dep_time = np.random.randint(6, 22)  # Flights between 6 AM and 10 PM
                    dep_delay = np.random.normal(15, 10)  # Average 15 min delay
                    
                    rows.append({
                        'FL_DATE': date,
                        'ORIGIN': origin,
                        'DEST': dest,
                        'DEP_TIME': float(dep_time * 100),  # Convert to HHMM format
                        'DEP_DELAY': int(dep_delay),
                        'AIR_TIME': int(max(30, air_time)),  # Ensure positive air time
                        'DISTANCE': int(np.random.normal(800, 200))  # Average flight distance
                    })
    
    # Create DataFrame
    flights_df = pd.DataFrame(rows)
    
    # Save to parquet
    output_path = 'flights_small.parquet'
    flights_df.to_parquet(output_path, index=False)
    
    # Print summary
    print(f"Generated {len(flights_df):,} rows of flight data")
    print(f"Date range: {flights_df['FL_DATE'].min()} to {flights_df['FL_DATE'].max()}")
    print(f"Origins: {', '.join(sorted(flights_df['ORIGIN'].unique()))}")
    print(f"Destinations: {', '.join(sorted(flights_df['DEST'].unique()))}")
    print(f"Saved to: {output_path}")
    
    # Show sample
    print("\nSample data:")
    print(flights_df.head())
    
    # Show schema
    print("\nSchema:")
    for col, dtype in flights_df.dtypes.items():
        print(f"{col}: {dtype}")

if __name__ == "__main__":
    generate_flights_data() 