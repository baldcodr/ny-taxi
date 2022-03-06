#!/usr/bin/env python
# coding: utf-8


import os
import pandas as pd
from sqlalchemy import create_engine
from time import time

import argparse

def main(params):
    user = params.user
    password = params.password
    host = params.host
    port = params.port
    db = params.db
    url = params.url
    table_name = params.table_name
    csv_file = 'output.csv'

    os.system(f"wget {url} -O {csv_file}")

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')

    df_iter = pd.read_csv(csv_file, iterator=True, chunksize=100000)
    df = next(df_iter)

    df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
    df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)

    df.head(n=0).to_sql(name=table_name, con=engine, if_exists='replace')
    df.to_sql(name=table_name, con=engine, if_exists='append')



    while True:

        try:
            starttime = time()
            df = next(df_iter)
            
            df.tpep_dropoff_datetime = pd.to_datetime(df.tpep_dropoff_datetime)
            df.tpep_pickup_datetime = pd.to_datetime(df.tpep_pickup_datetime)
            
            df.to_sql(name=table_name, con=engine, if_exists='append')

            endtime = time()
            print(f"time difference is: ",(endtime - starttime))

        except StopIteration:
            print("Finished ingesting data into the postgres database")
            break





if __name__ == '__main__':
    #add description
    parser = argparse.ArgumentParser(description='Import CSV data to postgres')

    #define arguments specifying the parameters
    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True,  help='password for postgres')
    parser.add_argument('--host', required=True,  help='host for postgres')
    parser.add_argument('--port', required=True,  help='port for postgres')
    parser.add_argument('--db', required=True,  help='database for postgres')
    parser.add_argument('--table_name', required=True,  help='table name for postgres')
    parser.add_argument('--url', required=True, help='URL of csv file')

    args = parser.parse_args()

    main(args)


