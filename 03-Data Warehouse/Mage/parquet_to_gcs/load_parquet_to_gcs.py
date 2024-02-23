import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):

    df_list = []
    for month in range(1,13):

        url = 'https://d37ci6vzurychx.cloudfront.net/trip-data/green_tripdata_2022-'+str(month).rjust(2,'0')+'.parquet'
        print(url) 
        
        taxi_dtypes = {
                        'VendorID': pd.Int64Dtype(),
                        'passenger_count': pd.Int64Dtype(),
                        'trip_distance': float,
                        'RatecodeID':pd.Int64Dtype(),
                        'store_and_fwd_flag':str,
                        'PULocationID':pd.Int64Dtype(),
                        'DOLocationID':pd.Int64Dtype(),
                        'payment_type': pd.Int64Dtype(),
                        'fare_amount': float,
                        'extra':float,
                        'mta_tax':float,
                        'tip_amount':float,
                        'tolls_amount':float,
                        'improvement_surcharge':float,
                        'total_amount':float,
                        'congestion_surcharge':float
                    }
        # native date parsing 
        parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

        temp = pd.read_parquet(url)
        temp = temp.astype(taxi_dtypes)
        temp[parse_dates] = temp[parse_dates].apply(pd.to_datetime)
        print(temp.shape[0])
        df_list.append(temp)

    df = pd.concat(df_list)
    print(df.shape[0])
    return df

@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
