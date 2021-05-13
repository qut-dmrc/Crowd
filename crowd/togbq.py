from google.cloud import bigquery
import os
import logging

logging.basicConfig(filename='info_bq.log', level=logging.INFO)
def append_to_bq(credentials, table_id, csv_file):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
    # Construct a BigQuery client object.
    client = bigquery.Client()
    table = bigquery.Table(client.project+'.'+table_id)
    # datasets = list(client.list_datasets())
    try:
        client.get_table(table_id) # check if the table exists
        print('Table exists')
    except:
        print('Creating new table')
        table = client.create_table(table)  # create the table if it doesn't

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        skip_leading_rows=1, 
        autodetect=True
    )
    job_config.allow_quoted_newlines = True

    with open(csv_file, "rb") as source_file:
        job = client.load_table_from_file(source_file, table_id, job_config=job_config)
        job.result()  # Waits for the job to complete.

    table = client.get_table(table_id)  # Make an API request.
    print(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
    logging.info(
        "Loaded {} rows and {} columns to {}".format(
            table.num_rows, len(table.schema), table_id
        )
    )
