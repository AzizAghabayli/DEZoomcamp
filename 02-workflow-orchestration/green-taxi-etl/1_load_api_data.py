import io
import pandas as pd
import requests
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test


@data_loader
def load_data_from_api(*args, **kwargs):
    """
    Template for loading data from API
    """

    # Green taxi dataset path
    url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/green/'

    # List of dataset files for months 10, 11, 12 of 2020
    datasets = [
        'green_tripdata_2020-10.csv.gz',
        'green_tripdata_2020-11.csv.gz',
        'green_tripdata_2020-12.csv.gz'
    ]

    # Empty list to store the DataFrames
    data_frames = []

    # Loop over the dataset filenames
    for dataset in datasets:
        file_path = f"{url}/{dataset}"
        df = pd.read_csv(file_path, compression='gzip')
        data_frames.append(df)
    
    # Concatenate DataFrames into a single DataFrame
    green_tripdata_df = pd.concat(data_frames, ignore_index=True)


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
    
    # Convert data types
    for column, dtype in taxi_dtypes.items():
        if column in green_tripdata_df.columns:
            green_tripdata_df[column] = green_tripdata_df[column].astype(dtype)

    # Parse dates
    parse_dates = ['lpep_pickup_datetime', 'lpep_dropoff_datetime']

    for date_column in parse_dates:
        if date_column in green_tripdata_df.columns:
            green_tripdata_df[date_column] = pd.to_datetime(green_tripdata_df[date_column])
    
    return green_tripdata_df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
