import os
from azure.storage.filedatalake import DataLakeServiceClient
from azure.identity import ClientSecretCredential
from numpy import source
import pandas as pd
from connectionazure.utils import read_file_as_bytes, write_file_as_bytes
from io import BytesIO
import concurrent.futures


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
    

    def list_directory_contents(self, container: str, path='') -> pd.DataFrame:
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
            directory['permissions'] = path.permissions
            directory['path'] = path.name
            directory['last_modified'] = path.last_modified
            directory['owner'] = path.owner
            directory['is_directory'] = path.is_directory
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
        """delete a container in the storage account.

        Args:
            container_name (str): container name to be delted.
        """
        self.service_client.delete_file_system(file_system=container_name)

    def create_directory(self, container: str, path: str):
        """create a directory in the container.

        Args:
            container (str): name of the container.
            path (str): path that will be created.
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        file_system_client.create_directory(path)

    def delete_directory(self, container: str, path: str):
        """delete directory in datalake.

        Args:
            container (str): name of the contaier
            path (str): path of the file to be deleted
        """
        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(path)
        directory_client.delete_directory()
    
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

        return True

    def check_if_path_exists(self, container, path, file_name):
        paths_df = self.list_directory_contents(container=container, path=path)

        if path=='/':
            path=''
        if len(paths_df)>0:
            if (path + '/' + file_name) in paths_df.path.values:
                return True
        return False

    def upload_file_to_directory(self, container: str, path: str, file_name: str, data: bytes, overwrite=False):
        """save a string to a file in datalake.

        Args:
            container (str): name of the container.
            path (str): path were it will be save.
            file_name (str): name of the file.
            data (bytes): data that will be save as binary.
            overwrite (bool, optional): if the file will be overwriten or not. Defaults to False.

        Raises:
            Exception: if the file already exists and overwrite equals false it will be raise.
        """
        if overwrite==False:
            resp = self.check_if_path_exists(container, path, file_name)
            if resp:
                raise Exception(f"{path + '/' + file_name} already exists, can be set overwrite=True to overwrite this file.")

        file_system_client = self.service_client.get_file_system_client(file_system=container)
        directory_client = file_system_client.get_directory_client(path)
        file_client = directory_client.create_file(file_name)

        file_contents = data

        file_client.append_data(data=file_contents, offset=0, length=len(file_contents))

        file_client.flush_data(len(file_contents))

    def upload_file_to_directory_bulk(self, container: str, path: str, file_name: str, data: bytes, overwrite=False):
        """Upload bigger files to data lake

        Args:
            container (str): name of the container.
            path (str): path were it will be save.
            file_name (str): name of the file.
            data (bytes): data that will be save as binary.
             overwrite (bool, optional): if the file will be overwriten or not. Defaults to False.
        """
        if overwrite==False:
            resp = self.check_if_path_exists(container, path, file_name)
            if resp:
                raise Exception(f"{path + '/' + file_name} already exists, can be set overwrite=True to overwrite this file.")

        file_system_client = self.service_client.get_file_system_client(file_system=container)

        directory_client = file_system_client.get_directory_client(path)
        
        file_client = directory_client.create_file(file_name)

        # overwrite must be set to True to end-point work
        file_client.upload_data(data, overwrite=True)

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

    def download_to_file(self, container: str, source_path: str, path_sink: str) -> bool:
        """download a file on datalake to a local file.

        Args:
            container (str): source container
            source_path (str): path of the file on the container
            path_sink (str): path that the file will be saved

        Returns:
            bool: True if the file was saved.
        """
        file_name = source_path.split('/')[-1]
        source_directory = '/'.join(source_path.split('/')[:-1])

        binary = self.download_file_as_binary(container, source_directory, file_name)

        write_file_as_bytes(path_sink, binary)

        return True

    def upload_directory_recursive(self, source_path: str, sink_container: str, sink_path: str) -> bool:
        """
        move all files from a folder to datalake.

        Parameters
        ----------
        source_path : str
            Folder where is the files that will be moved
        sink_container : str
            container that will be moved to
        sink_path: str
            path that the files will be moved
        """
        directories = os.listdir(source_path)

        for directory in directories:
            local_path = source_path + '/' + directory

            if os.path.isdir(local_path):
                sink_path_added_folder = sink_path + '/' + directory
                self.upload_directory_recursive(local_path, sink_container, sink_path_added_folder)

            else:
                file_content = read_file_as_bytes(local_path)
                self.upload_file_to_directory_bulk(container=sink_container, path=sink_path, file_name=directory, data=file_content, overwrite=True)
                print(f'{local_path} copied')

        return True

    def download_directory(self, source_container: str, source_path: str, sink_path: str) -> bool:
        """Download all files of a directory to a local folder.

        Args:
            source_container (str): container that the data is.
            source_path (str): path of the files that 
            sink_path (str): _description_

        Returns:
            bool: _description_
        """
        df_directory_list = self.list_directory_contents(source_container, source_path)
        df_only_file_list = df_directory_list[df_directory_list.is_directory==False].copy()

        source_path_list = source_path.split('/')
        source_path_list_clean = list(filter(lambda a: a != '', source_path_list))

        sink_path_list = sink_path.split('/')
        sink_path_list_clean = list(filter(lambda a: a != '', sink_path_list))

        df_only_file_list['local_path'] = '/'.join(sink_path_list_clean) + '/' + df_only_file_list.path.str.split('/').apply(lambda x: '/'.join(x[len(source_path_list_clean):]))

        with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
            for _, row in df_only_file_list.iterrows():
                executor.submit(self.download_to_file, source_container, row['path'], row['local_path'])
            

        return True

    def upload_dataframe_as_parquet(self, df: pd.DataFrame, container: str, sink_path: str, file_name: str, to_parquet_options_dict: dict={}) -> bool:
        """upload DataFrame to datalake as parquet.

        Args:
            df (pd.DataFrame): dataframe to be uploaded
            container (str): sink container
            sink_path (str): sink path
            file_name (str): name of the file that will be saved
            to_parquet_options_dict (str): options to transform the dataframe in parquet

        Raises:
            Exception: the upload will fail if the path arg on to_parquet_options_dict

        Returns:
            bool: True if the file was upload with success
        """
        if 'path' in to_parquet_options_dict.keys():
            raise Exception('The dataframe will not be saved in datalake if path is sended on kwargs')

        binary = df.to_parquet(**to_parquet_options_dict)

        self.upload_file_to_directory_bulk(container=container, path=sink_path, file_name=file_name, data=binary)

        return True

    def download_parquet_as_dataframe(self, container:str, source_path: str, file_name: str, read_parquet_options_dict:dict = {})-> pd.DataFrame:
        """download parquet binary as dataframe

        Args:
            container (str): source container
            source_path (str): source path
            file_name (str): file name
            read_parquet_options_dict (dict): options to read parquet binary as dataframe

        Returns:
            pd.DataFrame: dataframe object generate from binary on datalake
        """

        df_binary = self.download_file_as_binary(container=container, path=source_path, file_name=file_name)
        pq_file = BytesIO(df_binary)

        return pd.read_parquet(pq_file, **read_parquet_options_dict)
