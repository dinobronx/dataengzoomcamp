#!/usr/bin/env python
# coding: utf-8

import os
import pandas as pd
from sqlalchemy import create_engine
from time import time
from datetime import timedelta
from prefect import flow, task
from prefect.tasks import task_input_hash
from prefect_sqlalchemy import SqlAlchemyConnector


@task(log_prints=True, retries=3, cache_key_fn=task_input_hash, cache_expiration=timedelta(days=1))
def extract_data(csv_url):
       # temp csv
       if csv_url.endswith('.csv.gz'):
              csv_name = 'output.csv.gz'
       else:
              csv_name = 'output.csv'
       
       # download the csv
       os.system(f"wget {csv_url} -O {csv_name}")
       
       df_iter = pd.read_csv(csv_name, iterator=True, chunksize=100000)
       df = next(df_iter)

       df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
       df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
       return df

@task(log_prints=True)
def transform_data(df):
       # removing null ?
       print(f"pre: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
       df = df[df['passenger_count'] != 0]
       print(f"post: missing passenger count: {df['passenger_count'].isin([0]).sum()}")
       return df

@task(log_prints=True, retries=3)
def ingest_data(table_name, df):
       connection_block = SqlAlchemyConnector.load("postgres-connector")

       with connection_block.get_connection(begin=False) as engine:
              df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
              df.to_sql(name=table_name, con=engine, if_exists='append')


       # ok apparently we can delete the code below, because this will all run as a flow ? 
       # I still don't get it so Im making a note for now.

       # 8.24 I have the impression that im still gonna get all the data
       # 10.27 - we only got 100k rows, so the rest are not inserting, i have the impressiong that becuase its now a flow we are
       # still gonna get the rest of the data

       


       # while True:
       #        try:

       #               t_start = time()
       #               df = next(df_iter)
       #               df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
       #               df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
                     
       #               df.to_sql(name=table_name, con=engine, if_exists='append')
                     
       #               t_end = time()
       #               total = t_end - t_start
                     
       #               print(f'inserted another chunk ..., took {total}')
              
       #        except StopIteration:
       #               print('Finished ingesting data into the postgres database')
       #               break


@flow(name="Subflow", log_prints=True)
def log_subflow(table_name: str):
       print(f"Logging Subflow for: {table_name}")

# python decorator / there's a flow wrapped around it
@flow(name="Ingest Flow")
def main_flow(table_name: str):
       user = "root"
       password = "root"
       host = "localhost"
       port = "5432" 
       db = "ny_taxi" 
       csv_url = "https://github.com/DataTalksClub/nyc-tlc-data/releases/download/yellow/yellow_tripdata_2021-01.csv.gz"
       log_subflow(table_name)
       raw_data = extract_data(csv_url)
       data = transform_data(raw_data)
       ingest_data(table_name, data)

if __name__ == '__main__':
       table_name = "yellow_taxi_trips_prefect" 
       main_flow(table_name)       
       

