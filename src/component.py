'''
Template Component main class.

'''
import logging
import os
import json
from datetime import datetime
from pathlib import Path

import keboola.component.dao
from keboola.component import CommonInterface
from google_cloud_storage.client import StorageClient
from google.auth.exceptions import GoogleAuthError
from google.api_core.exceptions import NotFound

KEY_BUCKET_NAME = "bucket_name"
KEY_APPENDDATE = "append_date"
KEY_SERVICE_ACCOUNT = "#service_account_key"
KEY_FOLDER_NAME = "folder_name"

REQUIRED_PARAMETERS = [KEY_BUCKET_NAME]
REQUIRED_IMAGE_PARS = []

APP_VERSION = '1.0.0'


class UserException(Exception):
    pass


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
            self.validate_configuration_parameters(REQUIRED_PARAMETERS)
            self.validate_image_parameters(REQUIRED_IMAGE_PARS)
        except ValueError as e:
            logging.exception(e)
            exit(1)

    def run(self):
        params = self.configuration.parameters
        service_account_json_key = params.get(KEY_SERVICE_ACCOUNT)
        client_id_credentials = self.configuration.oauth_credentials
        bucket_name = params.get(KEY_BUCKET_NAME)
        folder_name = params.get(KEY_FOLDER_NAME, "")
        if folder_name and folder_name[-1] != "/":
            folder_name = f"{folder_name}/"

        if service_account_json_key:
            service_account_json_key = KeyCredentials(service_account_json_key).key

        try:
            storage_client = StorageClient(bucket_name,
                                           service_account_json_key=service_account_json_key,
                                           client_id_credentials=client_id_credentials)
        except ValueError as value_error:
            raise UserException(value_error)

        files_and_tables = self.get_files_and_tables()
        append_date = params[KEY_APPENDDATE]
        for file in files_and_tables:
            self.upload_file(storage_client, bucket_name, folder_name, file, append_date)

    def get_files_and_tables(self):
        in_tables = self.get_input_tables_definitions()
        in_files_per_tag = self.get_input_file_definitions_grouped_by_tag_group(only_latest_files=True)
        in_files = [item for sublist in in_files_per_tag.values() for item in sublist]
        return in_tables + in_files

    def upload_file(self, storage_client, bucket_name, destination_folder_name, file, append_date):
        try:
            source_file_path = file.full_path
            if os.path.isdir(source_file_path):
                self._process_folder_upload(local_folder_path=source_file_path,
                                            destination_folder_name=destination_folder_name,
                                            storage_client=storage_client,
                                            append_date=append_date,
                                            bucket_name=bucket_name)
            destination_blob_name = self._get_file_destination_name(destination_folder_name, file, append_date)
            storage_client.upload_blob(bucket_name, source_file_path, destination_blob_name)
        except GoogleAuthError as google_error:
            raise UserException(f"Upload failed after retries due to : {google_error}")
        except NotFound as e:
            raise UserException(f"Not Found error occurred, make sure Project and Folder in Google cloud exists: {e}")
        except ValueError as e:
            raise UserException(e) from e

    def _get_file_destination_name(self, destination_folder_name, file, append_date):
        if isinstance(file, keboola.component.dao.TableDefinition):
            src_folder = self.tables_in_path
        elif isinstance(file, keboola.component.dao.FileDefinition):
            src_folder = self.files_in_path
        else:
            raise UserException("Unknown file type.")

        return self._create_filename(file.full_path, src_folder, destination_folder_name, append_date)

    @staticmethod
    def _create_filename(full_path, src_folder, destination_folder_name, append_date):
        rel_path = os.path.relpath(full_path, src_folder)
        if destination_folder_name:
            rel_path = os.path.join(destination_folder_name, rel_path)

        timestamp_suffix = ''
        if append_date:
            timestamp_suffix = "_" + str(datetime.utcnow().strftime('%Y%m%d%H%M%S'))
        file_name, file_extension = os.path.splitext(os.path.basename(full_path))

        dst_path = os.path.dirname(rel_path)
        path_w_filename = os.path.join(dst_path, file_name)
        path_w_timestamp = path_w_filename + timestamp_suffix
        path_w_extension = path_w_timestamp + file_extension
        return path_w_extension

    def _process_folder_upload(self, local_folder_path, destination_folder_name,
                               append_date, bucket_name, storage_client):
        for dirpath, dirnames, filenames in os.walk(local_folder_path):
            for filename in filenames:
                if not filename.endswith(".manifest"):
                    full_path = os.path.join(dirpath, filename)
                    if os.path.isfile(full_path):
                        destination_blob_name = self._create_filename(full_path=full_path,
                                                                      src_folder=self.files_in_path,
                                                                      destination_folder_name=destination_folder_name,
                                                                      append_date=append_date)

                        storage_client.upload_blob(bucket_name, full_path, destination_blob_name)


class KeyCredentials:
    REQUIRED_KEY_PARAMETERS = ["client_email", "token_uri", "private_key", "project_id"]

    def __init__(self, key_string):
        self.key = self.parse_key_string(key_string)
        self.validate_key()

    @staticmethod
    def parse_key_string(key_string):
        try:
            key = json.loads(key_string, strict=False)
        except (ValueError, TypeError):
            raise UserException("The service account key format is incorrect, copy and paste the whole JSON content "
                                "of the key file into the text field")
        return key

    def validate_key(self):
        missing_fields = []
        for par in self.REQUIRED_KEY_PARAMETERS:
            if not self.key.get(par):
                missing_fields.append(par)

        if missing_fields:
            raise UserException(f'Google service account key is missing mandatory fields: {missing_fields} ')


"""
        Main entrypoint
"""
if __name__ == "__main__":
    try:
        comp = Component()
        comp.run()
    except UserException as exc:
        logging.exception(exc)
        exit(1)
    except Exception as exc:
        logging.exception(exc)
        exit(2)
