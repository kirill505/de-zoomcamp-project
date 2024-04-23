from mage_ai.settings.repo import get_repo_path
from mage_ai.io.config import ConfigFileLoader
from mage_ai.io.google_cloud_storage import GoogleCloudStorage
from os import path
if 'data_loader' not in globals():
    from mage_ai.data_preparation.decorators import data_loader
if 'test' not in globals():
    from mage_ai.data_preparation.decorators import test
from google.cloud import storage
import pandas as pd
import json


@data_loader
def load_from_google_cloud_storage(*args, **kwargs):
    """
    Template for loading data from a Google Cloud Storage bucket.
    Specify your configuration settings in 'io_config.yaml'.

    Docs: https://docs.mage.ai/design/data-loading#googlecloudstorage
    """
    posts_year = kwargs.get('posts_year')
    date_start = kwargs.get('date_start')
    date_end = kwargs.get('current_date')

    config_path = path.join(get_repo_path(), 'io_config.yaml')
    config_profile = 'default'
    bucket_name = 'producthunt_data'

    storage_client = storage.Client().from_service_account_json(
            '/home/src/default_repo/de-zoomcamp-project-ph-a054648a9793.json')

    blobs = storage_client.list_blobs(bucket_name, prefix=posts_year)
    items = [blob.name for blob in blobs]
    print(len(items), items)
    
    res = []
    for item in items:
        bucket = storage_client.bucket(bucket_name)

        blob = bucket.blob(item)

        contents = blob.download_as_bytes()
        if len(json.loads(contents.decode("utf-8"))) != 0:
            res += json.loads(contents.decode("utf-8"))

    df = pd.DataFrame(res)
    
    return df


@test
def test_output(output, *args) -> None:
    """
    Template code for testing the output of the block.
    """
    assert output is not None, 'The output is undefined'
