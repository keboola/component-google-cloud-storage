import logging
import os

import backoff
from google.api_core.exceptions import ServerError
from google.cloud import storage
from google.auth.transport import requests
from google.oauth2.credentials import Credentials as ClientIdCredentials
from google.oauth2.service_account import Credentials as ServiceCredentials

KEY_CLIENT_ID = "appKey"
KEY_CLIENT_SECRET = "appSecret"
KEY_REFRESH_TOKEN = "refresh_token"
KEY_AUTH_DATA = "data"
KEY_SERVICE_ACCOUNT = "#service_account_key"

CLIENT_ID_TOKEN_URI = "https://accounts.google.com/o/oauth2/token"


class StorageClient(storage.Client):

    def __init__(self, bucket_name, client_id_credentials=None, service_account_json_key=None):
        credentials, project_name = self._get_storage_credentials(bucket_name,
                                                                  client_id_credentials,
                                                                  service_account_json_key)
        super().__init__(credentials=credentials, project=project_name)
        self.log_messages = []

    def __del__(self):
        self.write_log_messages(print_rest=True)

    def _get_storage_credentials(self, bucket_name, client_id_credentials, service_account_json_key):
        if service_account_json_key:
            credentials, project_name = self._get_service_account_credentials(service_account_json_key)
        elif client_id_credentials:
            client_id = client_id_credentials[KEY_CLIENT_ID]
            client_secret = client_id_credentials[KEY_CLIENT_SECRET]
            refresh_token = client_id_credentials[KEY_AUTH_DATA][KEY_REFRESH_TOKEN]
            credentials, project_name = self._get_client_id_credentials(client_id,
                                                                        client_secret,
                                                                        refresh_token,
                                                                        bucket_name)
        else:
            raise ValueError("No Authentication method was filled in, either authorize via instant authorization "
                             "or a service account key.")
        return credentials, project_name

    @staticmethod
    def _get_client_id_credentials(client_id, client_secret, refresh_token, bucket_name):
        credentials = ClientIdCredentials(None, client_id=client_id,
                                          client_secret=client_secret,
                                          refresh_token=refresh_token,
                                          token_uri=CLIENT_ID_TOKEN_URI)
        request = requests.Request()
        credentials.refresh(request)
        return credentials, bucket_name

    @staticmethod
    def _get_service_account_credentials(service_account_credentials):
        try:
            credentials = ServiceCredentials.from_service_account_info(service_account_credentials)
        except ValueError:
            raise
        project_name = service_account_credentials["project_id"]
        logging.info(f"Uploading to Google Cloud Storage using {service_account_credentials['client_email']} "
                     f"service account")
        return credentials, project_name

    @backoff.on_exception(backoff.expo, ServerError, max_time=60)
    def upload_blob(self, bucket_name, source_file_path, destination_blob_name):
        bucket = self.bucket(bucket_name)
        blob = bucket.blob(destination_blob_name)
        if os.path.isfile(source_file_path):
            blob.upload_from_filename(source_file_path)
            self.write_log_messages(f"File {source_file_path} uploaded to {destination_blob_name}.\n")
        else:
            self.write_log_messages(f"Skipping: {source_file_path} - is a directory.\n")

    def write_log_messages(self, message='', print_rest=False):
        self.log_messages.append(message)

        if len(self.log_messages) >= 10 or print_rest:
            if self.log_messages:
                logging.info(''.join(self.log_messages))
                self.log_messages.clear()
