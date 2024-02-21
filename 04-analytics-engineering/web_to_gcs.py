import io
import os
import requests
import pandas as pd
from google.cloud import storage

# services = ['fhv','green','yellow']
init_url = 'https://github.com/DataTalksClub/nyc-tlc-data/releases/download/'
# switch out the bucketname
BUCKET = os.environ.get("GCP_GCS_BUCKET", "dbt_analytics_eng_aa")


def upload_to_gcs(bucket, object_name, local_file):
    client = storage.Client()
    bucket = client.bucket(bucket)
    blob = bucket.blob(object_name)
    blob.upload_from_filename(local_file)


def web_to_gcs(year, service):
    for i in range(12):
        
        # sets the month part of the file_name string
        month = '0'+str(i+1)
        month = month[-2:]

        # csv file_name
        csv_file_name = f"{service}_tripdata_{year}-{month}.csv.gz"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{csv_file_name}"
        r = requests.get(request_url)
        open(csv.file_name, 'wb').write(r.content)
        print(f"Local: {csv_file_name}")

        # read it back into a parquet file
        df = pd.read_csv(csv_file_name, compression='gzip')
        parquet_file_name = csv_file_name.replace('.csv.gz', '.parquet')
        df.to_parquet(parquet_file_name, engine='pyarrow')
        print(f"Parquet: {parquet_file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{parquet_file_name}", parquet_file_name)
        print(f"GCS: {service}/{parquet_file_name}")

        # delete local files
        os.remove(csv_file_name)
        os.remove(parquet_file_name)


web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'fhv')
# web_to_gcs('2020', 'fhv')