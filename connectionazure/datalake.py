import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
import pandas as pd


class ConnectionAzureDataLake:
    def __init__(self):
        pass

    def initialize_storage_account_ad_env_variable(self) -> None:
        """get cliend id, client secrect, tenant id and the storage account name from the enviroment variables and authenticat.
        """
        client_id = os.environ['client_id']
        client_secret = os.environ['client_secret']
        tenant_id = os.environ['tenant_id']
        storage_account_name = os.environ['storage_account_name']

        credential = ClientSecretCredential(tenant_id, client_id, client_secret)

        self.service_client = DataLakeServiceClient(account_url="{}://{}.dfs.core.windows.net".format(
            "https", storage_account_name), credential=credential)
    

    def list_directory_contents(self, container: str, path=''):
        """list all directory content.

        Args:
            container (str): container name.
            path (str, optional): path to that will be list content. Defaults to ''.

        Returns:
            DataFrame: return a dataframe with the permission, path, last modified data, owner and the name of the directory.
        """
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

    def list_containers(self) -> pd.DataFrame:
        """list containers in the storage account.

        Returns:
            pd.DataFrame: Returns a dataframe with the container name and the last date of modification.
        """
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

    def create_container(self, container_name: str) -> None:
        """create a new container in the storage account.

        Args:
            container_name (str): container name.
        """
        self.service_client.create_file_system(file_system=container_name)

    def delete_container(self, container_name: str) -> None:
        self.service_client.delete_file_system(file_system=container_name)

    def create_directory(self, container: str, path: str):
        """create a directory in the container.

        Args:
            container (str): name of the container.
            path (str): path that will be created.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        file_system_client.create_directory(path)

    def rename_directory(self, container: str, directory: str, new_directory_name: str):
        """rename directory in the datalake, it is the same of move a file.

        Args:
            container (str): name of the container.
            directory (str): old directory name.
            new_directory_name (str): new directory name.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(directory)
        new_dir_name = new_directory_name
        directory_client.rename_directory(directory_client.file_system_name + '/' + new_dir_name)

    def delete_directory(self, container: str, path: str):
        """delete directory in datalake.

        Args:
            container (str): name of the contaier
            path ([type]): path of the file to be deleted
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(path)
        directory_client.delete_directory()

    def save_string_to_file(self, container: str, path: str, file_name: str, string: str, overwrite=False):
        """save a string to a file in datalake.

        Args:
            container (str): name of the container.
            path (str): path were it will be save.
            file_name ([type]): name of the file.
            string ([type]): data that will be save as string.
            overwrite (bool, optional): if the file will be overwriten or not. Defaults to False.

        Raises:
            Exception: if the file already exists and overwrite equals false it will be raise.
        """
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

    def download_file_as_binary(self, container: str, path: str, file_name: str):
        """download file as binary.

        Args:
            container (str): name of the container.
            path (str): path of the file.
            file_name (str): file name.

        Returns:
            Binary: file as binary
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)

        directory_client = file_system_client.get_directory_client(path)

        file_client = directory_client.get_file_client(file_name)

        download = file_client.download_file()

        return download.readall()

    def download_file_as_string(self, container: str, path: str, file_name: str, encode='UTF-8'):
        """download file as string.

        Args:
            container (str): name of the container.
            path (str): path of the file.
            file_name (str): file name.
            encode (str, optional): type of encode of the data. Defaults to 'UTF-8'.

        Returns:
            string: file as string
        """
        downloaded_bytes = self.download_file_as_binary(container, path, file_name)

        return downloaded_bytes.decode(encode)