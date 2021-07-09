import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd


class ConnectionAzureDataLake:
    def __init__(self):
        pass

    def initialize_storage_account_ad_env_variable(self):
        client_id = os.environ['client_id']
        client_secret = os.environ['client_secret']
        tenant_id = os.environ['tenant_id']
        storage_account_name = os.environ['storage_account_name']

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    

    def list_directory_contents(self, container, path=''):
        file_system_client = self.service_client.get_file_system_client(file_system=container)

        paths = file_system_client.get_paths(path=path)

        directories = dict()
        count=0
        for path in paths:
            directory = dict()
            directory['permission'] = path.permissions
            directory['path'] = path.name
            directory['last_modified'] = path.last_modified
            directory['owner'] = path.owner
            directories[count] = directory.copy()
            count+=1

        return pd.DataFrame(directories).T

    def list_containers(self):
        containers = self.service_client.list_file_systems()

        all_containers_dict = dict()
        count=0
        for container in containers:
            containers_dict = dict()
            containers_dict['container'] = container.name
            containers_dict['last_modified'] = container.last_modified
            all_containers_dict[count] = containers_dict.copy()
            count+=1

        return pd.DataFrame(all_containers_dict).T

    def create_container(self, container_name):
        self.file_system_client = self.service_client.create_file_system(file_system=container_name)

    def create_directory(self, container, path):
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        file_system_client.create_directory(path)

    def rename_directory(self, container, directory, new_directory_name):
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(directory)
        new_dir_name = new_directory_name
        directory_client.rename_directory(directory_client.file_system_name + '/' + new_dir_name)

    def delete_directory(self, container, path):
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(path)
        directory_client.delete_directory()

    def save_string_to_file(self, container, path, file_name, string, overwrite=False):

        if overwrite==False:
            paths_df = self.list_directory_contents(container=container, path=path)
            if len(paths_df)>0:
                if (path + '/' + file_name) in paths_df.path.values:
                    raise Exception(f"{path + '/' + file_name} already exists, can be set overwrite=True to overwrite this file.")

        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)

        file_contents = string

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    def download_file_as_string(self, container, path, file_name, encode='UTF-8'):
        file_system_client = self.service_client.get_file_system_client(file_system=container)

        directory_client = file_system_client.get_directory_client(path)

        file_client = directory_client.get_file_client(file_name)

        download = file_client.download_file()

        downloaded_bytes = download.readall()

        return downloaded_bytes.decode(encode)