#!/usr/bin/env python
# coding: utf-8

# In[1]:

import pandas as pd
from sqlalchemy import create_engine
import click

dtype = {
    "VendorID": "Int64",
    "passenger_count": "Int64",
    "trip_distance": "float64",
    "RatecodeID": "Int64",
    "store_and_fwd_flag": "string",
    "PULocationID": "Int64",
    "DOLocationID": "Int64",
    "payment_type": "Int64",
    "fare_amount": "float64",
    "extra": "float64",
    "mta_tax": "float64",
    "tip_amount": "float64",
    "tolls_amount": "float64",
    "ehail_fee": "float64",
    "improvement_surcharge": "float64",
    "total_amount": "float64",
    "trip_type": "Int64",
    "congestion_surcharge": "float64"
}

parse_dates = [
    "lpep_pickup_datetime",
    "lpep_dropoff_datetime"
]

@click.command()
@click.option('--pg-user', default='root', help='PostgreSQL user')
@click.option('--pg-pass', default='root', help='PostgreSQL password')
@click.option('--pg-host', default='localhost', help='PostgreSQL host')
@click.option('--pg-port', type=int, default=5432, help='PostgreSQL port')
@click.option('--pg-db', default='ny_taxi', help='PostgreSQL database')
@click.option('--table', default='green_taxi_data', help='Table name to ingest data into')
@click.option('--year', type=int, default=2025, help='Year of data')
@click.option('--month', type=int, default=11, help='Month of data')
@click.option('--chunksize', type=int, default=100000, help='Chunk size for reading CSV')
def run(pg_user, pg_pass, pg_host, pg_port, pg_db, table, year, month, chunksize):
    url = 'green_tripdata_2025-11.parquet'

    engine = create_engine(f'postgresql://{pg_user}:{pg_pass}@{pg_host}:{pg_port}/{pg_db}')

    df = pd.read_parquet(url)
    
    # Apply dtypes
    df = df.astype(dtype)
    
    print(f'Ingesting green taxi data...')
    df.to_sql(name=table, con=engine, if_exists='replace')
    
    # Ingest taxi zones
    zones_df = pd.read_csv('taxi_zone_lookup.csv')
    print(f'Ingesting taxi zones...')
    zones_df.to_sql(name='taxi_zones', con=engine, if_exists='replace')

if __name__ == '__main__':
    run()