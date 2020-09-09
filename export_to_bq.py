from civis import io
import pandas as pd
import numpy as np
from google.cloud import bigquery
import os
import json
import google

## Initialize variables
db = os.environ['DB']
schema = os.environ['SCHEMA']
tables = os.environ['TABLES'].split(',')
bq_project = os.environ['BQ_PROJECT']
bq_dataset = os.environ['BQ_DATASET']
write_disposition = os.environ['WRITE_DISPOSITION'].upper()

## Extract credentials from environment variable and
## follow the Civis BQ to Redshift documentation on how to create the appropriate 
## account and credentials. No need to mess around w/ GCS though. 
keyfile_dict = json.loads(os.environ['GOOGLE_BQ_SERVICE_ACCT_PASSWORD'])
credentials = google.oauth2.service_account.Credentials.from_service_account_info(keyfile_dict)

## Create the BigQuery client object, and then set up a config
## file. Autodetect is the key element here, but also make sure to set
## "WRITE_TRUNCATE" because otherwise it'll append.
bq_client = bigquery.Client(bq_project,credentials)
job_config = bigquery.job.LoadJobConfig(autodetect=True,
                                        write_disposition=write_disposition
                                       )

## Do the thing! Obviously you can do whatever you want 
## with the SQL, etc. 
for table in tables:
    sql = 'select * from {}.{}'.format(schema,table)
    
    df = io.read_civis_sql(sql, 
                    db, 
                    use_pandas=True,  
                    client=None, 
                    credential_id=None, 
                    polling_interval=None, 
                    archive=False, 
                    hidden=True)
    
    load_job = bq_client.load_table_from_dataframe(df,'{}.{}.{}'.format(bq_project,bq_dataset,table),job_config=job_config)
