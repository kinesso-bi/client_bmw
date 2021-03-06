import requests
from google.api_core.exceptions import BadRequest, Conflict, NotFound
from google.cloud import bigquery
from google.oauth2 import service_account
from requests.models import Response
import os


def create_client():
    # TODO(developer): Set key_path to the path to the service account key file.
    path = os.path.dirname(os.path.realpath(__file__))
    key_path = "{}/service_account.json".format(path)
    credentials = service_account.Credentials.from_service_account_file(
        key_path, scopes=["https://www.googleapis.com/auth/cloud-platform"],
    )
    # Construct a BigQuery client object.
    client = bigquery.Client(credentials=credentials, project=credentials.project_id, )
    return client


def upload_data(dataset_name: str, table_name: str, input_data):
    # Call a BigQuery client object contructor.
    client = create_client()
    table_id = '{}.{}.{}'.format(client.project, dataset_name, table_name)

    # tell the client everything it needs to know to upload our csv
    table = client.get_table(table_id)
    job_config = bigquery.LoadJobConfig()
    job_config.source_format = bigquery.SourceFormat.CSV
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job_config.schema_update_options = [
        bigquery.SchemaUpdateOption.ALLOW_FIELD_RELAXATION
    ]

    job_config.schema = table.schema

    try:
        # load the csv into bigquery
        job = client.load_table_from_dataframe(input_data, table, job_config=job_config)
        job.result()  # Waits for table load to complete.
        print("Loaded {} rows into {}.".format(job.output_rows, table_id))

    except (BadRequest, Conflict, NotFound) as e:
        print('ERROR: {}'.format(e))


def get_stream(get_url, get_app_id=None, get_report_type=None):
    response = requests.get(url=get_url)
    if response.status_code != 200:
        print(response.status_code)
    else:
        stream: Response = response
        return stream
