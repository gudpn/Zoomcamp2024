

import os
import argparse

import pandas as pd
from sqlalchemy import create_engine
from time import time


## add 2 url to dl the data 
## dtablename is hardcoded
## add try catch in while True loop 


def main(params):
    user = params.user
    password = params.password
    host = params.host 
    port = params.port 
    db = params.db
   # table_name = params.table_name
    gurl = params.gurl
    turl = params.turl
        

    engine = create_engine(f'postgresql://{user}:{password}@{host}:{port}/{db}')
    engine.connect()

    if gurl.endswith('.csv.gz'):
        csv_name = 'output.csv.gz'
    else:
        csv_name = 'output.csv'

    os.system(f"wget {gurl} -O {csv_name}")

    if turl.endswith('.csv.gz'):
        csv_name1 = 'output1.csv.gz'
    else:
        csv_name1 = 'output1.csv'

    os.system(f"wget {turl} -O {csv_name1}")

    df_green_iter = pd.read_csv(csv_name, compression='gzip',iterator=True,chunksize=2000)


    df_import_green = next(df_green_iter)

    df_import_green.lpep_pickup_datetime= pd.to_datetime(df_import_green.lpep_pickup_datetime ) 
    df_import_green.lpep_dropoff_datetime = pd.to_datetime( df_import_green.lpep_dropoff_datetime)



    ##this will only have the eable columns name
    ## This is the line to create table -green_taxi
    df_import_green.head(n=0).to_sql(name='green_taxi',con=engine,if_exists ='replace')


    ## this is to insert data , with iter  ( the first batch)
    df_import_green.to_sql(name='green_taxi',con=engine,if_exists ='append')

    # second to last batch , it will exit with exception
    i = 2
    while True: 
        try:
            t_start = time()
            df_import_green = next(df_green_iter)
            df_import_green.lpep_pickup_datetime= pd.to_datetime(df_import_green.lpep_pickup_datetime ) 
            df_import_green.lpep_dropoff_datetime = pd.to_datetime( df_import_green.lpep_dropoff_datetime)
            df_import_green.to_sql(name='green_taxi',con=engine,if_exists ='append')
            t_end = time()
            print('inserted ' ,i,' batch'   , (t_end - t_start))
            i+= 1 
        except StopIteration:
            break
            


    df_zone_iter  = pd.read_csv(csv_name1,iterator=True,chunksize=100)
    df_import_zone = next(df_zone_iter)

    df_import_zone.head(n=0).to_sql(name='taxi_zone',con=engine,if_exists ='replace')
    df_import_zone.to_sql(name='taxi_zone',con=engine,if_exists ='append')
    i = 2
    while True: 
        try:
            t_start = time()
            df_import_zone = next(df_zone_iter)
            df_import_zone.to_sql(name='taxi_zone',con=engine,if_exists ='append')
            t_end = time()
            print('inserted ' ,i,' batch in taxi_zone'   , (t_end - t_start))
            i+= 1 
        except StopIteration:
            break



if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Ingest CSV data to Postgres')

    parser.add_argument('--user', required=True, help='user name for postgres')
    parser.add_argument('--password', required=True, help='password for postgres')
    parser.add_argument('--host', required=True, help='host for postgres')
    parser.add_argument('--port', required=True, help='port for postgres')
    parser.add_argument('--db', required=True, help='database name for postgres')
   # parser.add_argument('--table_name', required=True, help='name of the table where we will write the results to')
    parser.add_argument('--gurl', required=True, help='url of the green taxi csv file')
    parser.add_argument('--turl', required=True, help='url of the taxi zone csv file')
    args = parser.parse_args()

    main(args)
