import io
import os
import requests
import pandas as pd
from google.cloud import storage

# services = ['fhv','green','yellow']
init_url = 'https://www.nyc.gov/site/tlc/about/tlc-trip-record-data.page'
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

        # file_name
        file_name = f"{service}_tripdata_{year}-{month}.parquet"

        # download it using requests via a pandas df
        request_url = f"{init_url}{service}/{file_name}"
        r = requests.get(request_url)
        # open(file_name, 'wb').write(r.content)
        with open(file_name, 'wb') as file:
            file.write(r.content)
        print(f"Local: {file_name}")

        # upload it to gcs 
        upload_to_gcs(BUCKET, f"{service}/{file_name}", file_name)
        print(f"GCS: {service}/{file_name}")


# web_to_gcs('2019', 'yellow')
# web_to_gcs('2020', 'yellow')
# web_to_gcs('2019', 'green')
# web_to_gcs('2020', 'green')
# web_to_gcs('2019', 'fhv')
# web_to_gcs('2020', 'fhv')