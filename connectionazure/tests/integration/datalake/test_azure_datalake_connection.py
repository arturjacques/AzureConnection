from connectionazure.tests.integration.integration_base_test import IntegrationBaseTest
from connectionazure.datalake import ConnectionAzureDataLake
from pandas import DataFrame
import os

from random import randint

class ConnectionAzureDataLakeTest(IntegrationBaseTest):       
    def setUp(self) -> None:
        super().setUp()

        self.datalake_connection = ConnectionAzureDataLake()
        self.datalake_connection.initialize_storage_account_ad_env_variable()
        self.test_container = 'test-validation-container'
        self.dummy_download_file = {'container': self.test_container, 'path':'test_folder', 'file_name':'test_file.txt'}

    def test_list_containers(self):
        try:
            container_list = self.datalake_connection.list_containers()
        except:
            container_list = None
        finally:
            self.assertTrue(type(container_list)==DataFrame)
        
    def test_create_and_delete_container(self):
        container_name = f'teste-container-{randint(0, 9999)}'

        self.datalake_connection.create_container(container_name)
        container_created = container_name in self.datalake_connection.list_containers().container.to_list()
        self.assertTrue(container_created)

        self.datalake_connection.delete_container(container_name)
        container_deleted = container_name not in self.datalake_connection.list_containers().container.to_list()
        self.assertTrue(container_deleted)

    def test_list_directory_contents(self):
        directory = self.datalake_connection.list_directory_contents(container=self.test_container)

        self.assertTrue(len(directory)>=0)

    def test_create_directory_and_delete(self):
        directory_name = f'teste-folder-{randint(0, 9999)}'

        self.datalake_connection.create_directory(container=self.test_container, path=directory_name)

        directory_created = directory_name in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(directory_created)
        
        self.datalake_connection.delete_directory(container=self.test_container, path=directory_name)

    def test_upload_and_rename_directory(self):
        upload_file_name = f'test_rename_folder/teste-file-{randint(0, 9999)}.txt'
        upload_file_path = '/'
        file_content = b'Hello from test upload file to directory'
        new_directory_rename = f'teste-rename-{randint(0, 9999)}'

        self.datalake_connection.upload_file_to_directory_bulk(container=self.test_container,
                                                                path=upload_file_path,
                                                                file_name=upload_file_name,
                                                                data=file_content)

        
        self.datalake_connection.rename_directory(container=self.test_container,
                                                  directory=upload_file_path + '/' + upload_file_name,
                                                  new_directory_name=new_directory_rename)

        directory_renamed = new_directory_rename in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(directory_renamed)

        self.datalake_connection.delete_directory(container=self.test_container,
                                                  path=new_directory_rename)

    def test_upload_file_to_directory(self):
        file_name = f'teste-file-{randint(0, 9999)}.txt'

        file_content = b'Hello from test upload file to directory'

        self.datalake_connection.upload_file_to_directory(container=self.test_container,
                                                          path='/',
                                                          file_name=file_name,
                                                          data=file_content)

        file_created = file_name in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(file_created)

        self.datalake_connection.delete_directory(container=self.test_container, path=file_name)

    def test_upload_file_to_directory_bulk(self):
        file_name = f'teste-file-{randint(0, 9999)}.txt'

        file_content = b'Hello from test upload file to directory'

        self.datalake_connection.upload_file_to_directory_bulk(container=self.test_container,
                                                          path='/',
                                                          file_name=file_name,
                                                          data=file_content,
                                                          overwrite=True)

        file_created = file_name in self.datalake_connection.list_directory_contents(container=self.test_container).path.to_list()

        self.assertTrue(file_created)

        self.datalake_connection.delete_directory(container=self.test_container, path=file_name)

    def test_download_file_as_binary(self):
        download = self.datalake_connection.download_file_as_binary(container=self.dummy_download_file['container'],
                                                                    path=self.dummy_download_file['path'],
                                                                    file_name=self.dummy_download_file['file_name'])


        expected = b'the data is correct on this file'
        self.assertEqual(download, expected)

    def test_download_file_as_string(self):
        download = self.datalake_connection.download_file_as_string(container=self.dummy_download_file['container'],
                                                                    path=self.dummy_download_file['path'],
                                                                    file_name=self.dummy_download_file['file_name'])


        expected = 'the data is correct on this file'
        self.assertEqual(download, expected)

    def test_upload_directory_recursive(self):
        sink_path = f'recursive_test_{randint(0, 9999)}'
        test_container = self.test_container

        os.mkdir('tmp')
        os.mkdir('tmp/first_folder')
        os.mkdir('tmp/second_folder')
        os.mkdir('tmp/first_folder/inner_folder')

        expected_path_result = [
            f'{sink_path}/file_tmp.txt',
            f'{sink_path}/first_folder/inner_folder/inner_file.txt',
            f'{sink_path}/second_folder/second_folder_file.txt'
        ]

        with open('tmp/file_tmp.txt', 'w') as f:
            f.write('tmp_file has this data')

        with open('tmp/first_folder/inner_folder/inner_file.txt', 'w') as f:
            f.write('inner_file has this data')

        with open('tmp/second_folder/second_folder_file.txt', 'w') as f:
            f.write('second_folder_file has this data')

        self.datalake_connection.upload_directory_recursive('tmp', test_container, sink_path)

        directory_list = self.datalake_connection.list_directory_contents(test_container, sink_path).path.to_list()

        delete_folder('tmp')
        self.datalake_connection.delete_directory(test_container, sink_path)

        for directory in expected_path_result:
            self.assertTrue(directory in directory_list)

def delete_folder(path):
    remove_files(path)
    remove_empty_folders(path)

def remove_files(root_path):
    new_paths = os.listdir(root_path)

    for path in new_paths:
        local_path = root_path + '/' + path
        if os.path.isdir(local_path):
            remove_files(local_path)
        else:
            os.remove(local_path)

def remove_empty_folders(root_path):
    new_paths = os.listdir(root_path)

    if len(new_paths)>0:
        for path in new_paths:
            local_path = root_path + '/' + path
            remove_empty_folders(local_path)
    else:
        os.removedirs(root_path)
        

