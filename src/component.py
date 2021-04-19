'''
Template Component main class.

'''
import logging
import os
from pathlib import Path
import requests
from urllib.parse import urlencode

from keboola.component import CommonInterface
from keboola.http_client import HttpClient

KEY_BUCKET_NAME = "bucket_name"
KEY_CLIENT_ID = "client_id"
KEY_CLIENT_SECRET = "#client_secret"
KEY_REFRESH_TOKEN = "#refresh_token"

API_PATH = "https://storage.googleapis.com/upload/storage/v1/b/"
API_PATH_SUFFIX = "/o"

ACCESS_TOKEN_REQUEST_URL = "https://oauth2.googleapis.com/"

# list of mandatory parameters => if some is missing,
# component will fail with readable message on initialization.
REQUIRED_PARAMETERS = [KEY_BUCKET_NAME, KEY_CLIENT_ID, KEY_CLIENT_SECRET, KEY_REFRESH_TOKEN]
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

        access_token = "Bearer " + self.get_access_token(params)
        bucket_name = params.get(KEY_BUCKET_NAME)
        base_url = API_PATH + bucket_name + API_PATH_SUFFIX
        cl = HttpClient(base_url)

        in_tables = self.get_input_tables_definitions()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_files = [item for sublist in in_files_per_tag.values() for item in sublist]

        for fl in in_tables + in_files:
            self._upload_file(fl, cl, access_token)

    def _upload_file(self, file, client, access_token):
        post_params = {"uploadType": "media", "name": file.name}
        post_headers = {"Authorization": access_token, "Content-Type": "text/csv"}
        with open(file.full_path, "rb") as a_file:
            file_dict = {file.name: a_file}
            response = client.post(files=file_dict, headers=post_headers, params=post_params)
            print(response)

    @staticmethod
    def get_access_token(params):
        access_cl = HttpClient(ACCESS_TOKEN_REQUEST_URL)
        access_token_body = {
            "client_id": params.get(KEY_CLIENT_ID),
            "grant_type": "refresh_token",
            "client_secret": params.get(KEY_CLIENT_SECRET),
            "refresh_token": params.get(KEY_REFRESH_TOKEN)
        }
        access_token_headers = {
            'content-type': 'application/x-www-form-urlencoded'
        }
        response = access_cl.post("token", headers=access_token_headers, data=access_token_body)

        access_token = response["access_token"]
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
