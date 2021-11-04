```
from google.cloud import storage
import pandas as pd
import numpy as np
import os

BUCKET_NAME = 'dshackeventdata'
PREFIX_FILE_NAME = 'CPN/Separate Patient FIles/'
client = storage.Client()
bucket = client.get_bucket(BUCKET_NAME)
data_dict_list = []

for blob in bucket.list_blobs(prefix=PREFIX_FILE_NAME):
    if blob.name.endswith(".csv"):
        print('file name:', str(blob.name))
        df = pd.read_csv("gs://{}/{}".format(BUCKET_NAME, blob.name))
        if 'Patient ID' not in df:
            df['Patient ID'] = os.path.basename(blob.name).replace('.csv', '')
            bucket.blob("{}".format(blob.name)).upload_from_string(df.to_csv(index=False), 'text/csv')
        data_dict = {'file_name': blob.name, 'num_columns': df.shape[0], 'num_rows': df.shape[1]}
        data_dict_list.append(data_dict)

df = pd.DataFrame(data_dict_list)
```

```
import random
import time
from google.cloud import bigquery
from google.cloud import storage

bq_client = bigquery.Client()
table_id = "hca-cti-ds-hackathon.f4_fhm.test_load_400_patients_1"

schema = [
    bigquery.SchemaField("datetime", "STRING"),
    bigquery.SchemaField("datatype", "STRING"),
    bigquery.SchemaField("monitorid", "STRING"),
    bigquery.SchemaField("sensortype", "STRING")
]

for i in range(1, 242):
    schema.append(bigquery.SchemaField("data_value_{}".format(i), "STRING"))

job_config = bigquery.LoadJobConfig(
    schema=schema,
    source_format=bigquery.SourceFormat.CSV,
)

BUCKET_NAME = 'dshackeventdata'
PREFIX_FILE_NAME = 'CPN/Separate Patient FIles/'

gcs_client = storage.Client()
bucket = gcs_client.get_bucket(BUCKET_NAME)

def retry_load_csv_into_bq_with_backoff(fn, uri, retries = 7, backoff_in_seconds = 1):
  x = 0
  while True:
    try:
      return fn(uri=uri)
    except:
      if x == retries-1:
        raise
      else:
        sleep = (backoff_in_seconds * 2 ** x +
                 random.uniform(0, 1))
        time.sleep(sleep)
        x += 1

def load_csv_into_table(uri):
    load_job = bq_client.load_table_from_uri(uri, table_id, job_config=job_config)  # Make an API request.
    return load_job

for blob in bucket.list_blobs(prefix=PREFIX_FILE_NAME):
    if blob.name.endswith(".csv"):
        print('\nfile name:', blob.name)
        uri = "gs://{}/{}".format(BUCKET_NAME, blob.name)
        print('\t-{}'.format(uri))
        load_job = retry_load_csv_into_bq_with_backoff(fn=load_csv_into_table, uri=uri)
        load_job.result()  # Waits for the job to complete.
```
