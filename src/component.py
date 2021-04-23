'''
Template Component main class.

'''
import logging
import os
import json
from datetime import datetime
from pathlib import Path
from keboola.component import CommonInterface
from google.cloud import storage
from google.auth.transport import requests
from google.oauth2.credentials import Credentials as ClientIdCredentials
from google.oauth2.service_account import Credentials as ServiceCredentials

KEY_BUCKET_NAME = "bucket_name"
KEY_CLIENT_ID = "appKey"
KEY_CLIENT_SECRET = "appSecret"
KEY_REFRESH_TOKEN = "refresh_token"
KEY_AUTH_DATA = "data"
KEY_APPENDDATE = "append_date"
KEY_SERVICE_ACCOUNT = "#service_account_key"

CLIENT_ID_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_BUCKET_NAME]
REQUIRED_IMAGE_PARS = []

APP_VERSION = '0.0.1'


def get_local_data_path():
    return Path(__file__).resolve().parent.parent.joinpath('data').as_posix()


def get_data_folder_path():
    data_folder_path = None
    if not os.environ.get('KBC_DATADIR'):
        data_folder_path = get_local_data_path()
    return data_folder_path


class Component(CommonInterface):
    def __init__(self):
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)
        try:
            self.validate_configuration(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

    def run(self):
        '''
        Main execution code
        '''
        params = self.configuration.parameters
        service_account_credentials = params.get(KEY_SERVICE_ACCOUNT)
        client_id_credentials = self.configuration.oauth_credentials
        bucket_name = params.get(KEY_BUCKET_NAME)
        storage_client = StorageClient.get_storage_client(bucket_name,
                                                          service_account_credentials=service_account_credentials,
                                                          client_id_credentials=client_id_credentials)

        files_and_tables = self.get_files_and_tables()
        append_date = params[KEY_APPENDDATE]
        for file in files_and_tables:
            self.upload_file(storage_client, bucket_name, file, append_date)

    def get_files_and_tables(self):
        in_tables = self.get_input_tables_definitions()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_files = [item for sublist in in_files_per_tag.values() for item in sublist]
        return in_tables + in_files

    def upload_file(self, storage_client, bucket_name, file, append_date):
        source_file_path = file.full_path
        destination_blob_name = self._get_file_destination_name(file.name, append_date)
        self._upload_blob(storage_client, bucket_name, source_file_path, destination_blob_name)

    @staticmethod
    def _get_file_destination_name(file_path, append_date):
        timestamp_suffix = ''
        if append_date:
            timestamp_suffix = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))
        file_name, file_extension = os.path.splitext(os.path.basename(file_path))
        new_file_name = file_name + timestamp_suffix + file_extension
        return new_file_name

    @staticmethod
    def _upload_blob(storage_client, bucket_name, source_file_path, destination_blob_name):
        bucket = storage_client.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        blob.upload_from_filename(source_file_path)
        logging.info(f"File {source_file_path} uploaded to {destination_blob_name}.")


class StorageClient:

    @staticmethod
    def get_storage_client(bucket_name, client_id_credentials=None, service_account_credentials=None):

        if service_account_credentials:
            service_account_key = KeyCredentials(service_account_credentials).key
            storage_client = StorageClient._get_service_account_storage_client(service_account_key)

        elif client_id_credentials:
            client_id = client_id_credentials[KEY_CLIENT_ID]
            client_secret = client_id_credentials[KEY_CLIENT_SECRET]
            refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]
            storage_client = StorageClient._get_client_id_storage_client(client_id, client_secret, refresh_token,
                                                                         bucket_name)
        else:
            raise ValueError("No Authentication method was filled in, either authorize via instant authorization "
                             "or a service account key.")
        return storage_client

    @staticmethod
    def _get_client_id_storage_client(client_id, client_secret, refresh_token, bucket_name):
        credentials = ClientIdCredentials(None, client_id=client_id,
                                          client_secret=client_secret,
                                          refresh_token=refresh_token,
                                          token_uri=CLIENT_ID_TOKEN_URI)
        request = requests.Request()
        credentials.refresh(request)
        storage_client = storage.Client(credentials=credentials, project=bucket_name)
        return storage_client

    @staticmethod
    def _get_service_account_storage_client(service_account_credentials):
        credentials = ServiceCredentials.from_service_account_info(service_account_credentials)
        storage_client = storage.Client(credentials=credentials, project=service_account_credentials["project_id"])
        logging.info(f"Uploading to Google Cloud Storage using {service_account_credentials['client_email']} "
                     f"service account")
        return storage_client


class KeyCredentials:
    REQUIRED_KEY_PARAMETERS = ["client_email", "token_uri", "private_key", "project_id"]

    def __init__(self, key_string):
        self.key = self.parse_key_string(key_string)
        self.validate_key()

    @staticmethod
    def parse_key_string(key_string):
        try:
            key = json.loads(key_string, strict=False)
        except:
            raise ValueError("The service account key format is incorrect, copy and paste the whole JSON content "
                             "of the key file into the text field")
        return key

    def validate_key(self):
        missing_fields = []
        for par in self.REQUIRED_KEY_PARAMETERS:
            if not self.key.get(par):
                missing_fields.append(par)

        if missing_fields:
            raise ValueError(f'Google service account key is missing mandatory fields: {missing_fields} ')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except Exception as exc:
        logging.exception(exc)
        exit(2)
