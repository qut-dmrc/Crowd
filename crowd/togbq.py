from google.cloud import bigquery
import os

from typing import List, get_args, get_origin, Collection
import datetime as dt


# (field_type, mode)
# mode docs: https://cloud.google.com/bigquery/docs/reference/rest/v2/tables \
#   #TableFieldSchema.FIELDS.mode
BIGQUERY_TYPE_MAPPING = {
    str: "STRING",
    dt.datetime: "DATETIME",
    int: "INTEGER",
    float: "FLOAT",
    bool: "BOOL"
}


def get_table_schema(field_list: dict):
    """
    Turns a field list mapping column names to Python types into a BigQuery TableSchema
    object

    See https://cloud.google.com/bigquery/docs/reference/rest/v2/tables#TableSchema
    """
    fields = []

    for field_name, field_type in field_list.items():
        field_mode = "NULLABLE"
        # Assumes all Lists/Collections only have one argument
        try:
            if len(get_args(field_type)) == 1 \
                    and issubclass(get_origin(field_type), Collection):
                field_type = get_args(field_type)[0]
                field_mode = "REPEATED"
        except TypeError:
            print(f"field type {field_type} is problematic")
            raise
        try:
            fields.append(bigquery.schema.SchemaField(
                field_name,
                BIGQUERY_TYPE_MAPPING[field_type],
                mode=field_mode
            ))
        except KeyError:
            print(
                f"Can't convert {field_type} to a BigQuery column type (field name "
                f"{field_name})"
            )
            raise

    return fields


def append_to_bq(credentials, table_id, csv_file, field_list):
    os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = credentials
    # Construct a BigQuery client object.
    client = bigquery.Client()
    table = bigquery.Table(client.project+'.'+table_id)
    # datasets = list(client.list_datasets())
    try:
        client.get_table(table_id)  # check if the table exists
        print('Table exists')
    except:
        print('Creating new table')
        table = client.create_table(table)  # create the table if it doesn't

    job_config = bigquery.LoadJobConfig(
        source_format=bigquery.SourceFormat.CSV, 
        skip_leading_rows=1, 
        schema=get_table_schema(field_list)
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
