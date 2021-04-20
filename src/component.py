'''
Template Component main class.

'''
import logging
import mimetypes
import os
from datetime import datetime
from pathlib import Path
from keboola.component import CommonInterface
from keboola.http_client import HttpClient

KEY_BUCKET_NAME = "bucket_name"
KEY_CLIENT_ID = "appKey"
KEY_CLIENT_SECRET = "appSecret"
KEY_REFRESH_TOKEN = "refresh_token"
KEY_APPENDDATE = "append_date"

STORAGE_UPLOAD_URL = "https://storage.googleapis.com/upload/storage/v1/b/"
STORAGE_UPLOAD_ENDPOINT = "o?"

ACCESS_TOKEN_REQUEST_URL = "https://oauth2.googleapis.com/"

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
        # for easier local project setup
        data_folder_path = get_data_folder_path()
        super().__init__(data_folder_path=data_folder_path)

        try:
            # validation of required parameters. Produces ValueError
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

        oauth_credentials = self.configuration.oauth_credentials
        access_token = self._fetch_access_token(oauth_credentials)

        bucket_name = params.get(KEY_BUCKET_NAME)
        base_url = STORAGE_UPLOAD_URL + bucket_name
        http_client = HttpClient(base_url)

        files_and_tables = self.get_files_and_tables()

        for file in files_and_tables:
            self._upload_file(file, http_client, access_token)

    def get_files_and_tables(self):
        in_tables = self.get_input_tables_definitions()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_files = [item for sublist in in_files_per_tag.values() for item in sublist]
        return in_tables + in_files

    def _upload_file(self, file, client, access_token):
        file_name = self._get_file_name(file.name)
        post_params = {"uploadType": "media", "name": file_name}
        content_type = self._get_content_type(file.full_path)
        post_headers = {"Authorization": access_token, "Content-type": content_type}

        with open(file.full_path, "rb") as a_file:
            file_dict = {file_name: a_file}
            response = client.post(STORAGE_UPLOAD_ENDPOINT, files=file_dict, headers=post_headers, params=post_params)
            logging.info(f"Uploaded {file.full_path} to {response['bucket']}")

    def _get_file_name(self, file_path):
        params = self.configuration.parameters
        timestamp_suffix = ''
        if params[KEY_APPENDDATE]:
            timestamp_suffix = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))

        file_name, file_extension = os.path.splitext(os.path.basename(file_path))
        new_file_name = file_name + timestamp_suffix + file_extension
        return new_file_name

    @staticmethod
    def _get_content_type(file_name):
        return mimetypes.guess_type(file_name, strict=True)[0]

    @staticmethod
    def _fetch_access_token(oauth):
        access_cl = HttpClient(ACCESS_TOKEN_REQUEST_URL)
        access_token_body = {
            "client_id": oauth[KEY_CLIENT_ID],
            "grant_type": "refresh_token",
            "client_secret": oauth[KEY_CLIENT_SECRET],
            "refresh_token": oauth["data"][KEY_REFRESH_TOKEN]
        }
        access_token_headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }
        response = access_cl.post("token", headers=access_token_headers, data=access_token_body)

        access_token = "Bearer " + response["access_token"]
        return access_token


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
